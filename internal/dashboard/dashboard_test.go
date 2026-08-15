package dashboard_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/dashboard"
	"github.com/VeryFach/distributed-counter/internal/gateway"
	"github.com/VeryFach/distributed-counter/test/harness"
)

func TestDashboardServesHTML(t *testing.T) {
	d := dashboard.New(dashboard.Options{NodeID: "node-0"})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	d.Index(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("index status: %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "Cluster Dashboard") {
		t.Fatalf("index does not contain dashboard title")
	}
	if !strings.Contains(body, "cytoscape") {
		t.Fatalf("index does not reference cytoscape for topology")
	}
}

func TestDashboardClusterView(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cluster := harness.Start(t, 3, harness.Options{GossipInterval: 100 * time.Millisecond})
	cluster.StartLeaderElection(ctx)

	// Create a couple of counters across nodes.
	cluster.Increment(ctx, t, 0, 7)
	cluster.Increment(ctx, t, 1, 3)

	client, closeConn, err := cluster.Client(ctx, 0)
	if err != nil {
		t.Fatalf("dial node 0: %v", err)
	}
	if _, err := client.CreateCounter(ctx, &pb.CreateCounterRequest{Name: "post_1", Tags: []string{"likes"}}); err != nil {
		t.Fatalf("create counter: %v", err)
	}
	closeConn()

	// Let gossip + election settle.
	cluster.WaitConverged(t, 10, 15*time.Second)
	time.Sleep(500 * time.Millisecond)

	d := dashboard.New(dashboard.Options{
		NodeID:     "node-0",
		Membership: cluster.Nodes[0].Membership,
		Service:    cluster.Nodes[0].Service,
	})

	view := d.BuildClusterView(ctx)
	for _, c := range view.Counters {
		t.Logf("counter %s value=%d shard=%d", c.Name, c.Value, c.Shard)
	}
	if view == nil {
		t.Fatal("nil cluster view")
	}
	if len(view.Nodes) != 3 {
		t.Fatalf("expected 3 nodes, got %d: %+v", len(view.Nodes), view.Nodes)
	}
	if view.Self == nil || view.Self.NodeID != "node-0" {
		t.Fatalf("unexpected self: %+v", view.Self)
	}

	foundDefault := false
	foundPost := false
	for _, c := range view.Counters {
		if c.Name == "default" && c.Value == 10 {
			foundDefault = true
		}
		if c.Name == "post_1" {
			foundPost = true
		}
	}
	if !foundDefault {
		t.Fatalf("default counter not aggregated to 10: %+v", view.Counters)
	}
	if !foundPost {
		t.Fatalf("post_1 counter not listed: %+v", view.Counters)
	}
}

func TestDashboardClusterHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cluster := harness.Start(t, 1, harness.Options{GossipInterval: 100 * time.Millisecond})
	cluster.Increment(ctx, t, 0, 5)

	d := dashboard.New(dashboard.Options{
		NodeID:     "node-0",
		Membership: cluster.Nodes[0].Membership,
		Service:    cluster.Nodes[0].Service,
	})

	gw := gateway.New(cluster.Nodes[0].Service, gateway.Options{
		Port:      0,
		Dashboard: d,
	})
	srv := httptest.NewServer(gw.Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/cluster")
	if err != nil {
		t.Fatalf("get /api/cluster: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}

	var view dashboard.ClusterView
	if err := json.NewDecoder(resp.Body).Decode(&view); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(view.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(view.Nodes))
	}
}
