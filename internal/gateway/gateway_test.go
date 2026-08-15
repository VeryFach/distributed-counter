package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VeryFach/distributed-counter/internal/admin"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/gossip"
	"github.com/VeryFach/distributed-counter/internal/service"
	"go.uber.org/zap"
)

type testResponse struct {
	NodeId       string `json:"nodeId"`
	CounterName  string `json:"counterName"`
	CurrentValue string `json:"currentValue"`
}

func newTestGateway(t *testing.T) (*httptest.Server, *service.CounterService, *cluster.Membership, *gossip.GossipEngine) {
	t.Helper()

	log := zap.NewNop()
	svc := service.NewCounterService("test-node", 0, log)
	mem := cluster.NewMembership("test-node")
	mem.AddMember("test-node", "localhost:50051")
	mem.AddMember("peer-1", "localhost:50052")
	mem.AddMember("peer-2", "localhost:50053")
	svc.SetCluster(mem)

	eng := gossip.NewGossipEngine("test-node", svc.Counter(), svc.Clock(), mem, 50*time.Millisecond, log)

	adminSvc := admin.New("test-node", log)
	adminSvc.SetCluster(mem)
	adminSvc.SetGossip(eng)

	gw := New(svc, Options{
		Port:         0,
		AdminService: adminSvc,
		Logger:       log,
	})
	srv := httptest.NewServer(gw.Handler())
	t.Cleanup(srv.Close)

	return srv, svc, mem, eng
}

func doJSON(t *testing.T, method, url string, body any) *http.Response {
	t.Helper()

	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	return resp
}

func decode[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return out
}

func TestGatewayIncrementAndGetValue(t *testing.T) {
	srv, _, _, _ := newTestGateway(t)

	resp := doJSON(t, http.MethodPost, srv.URL+"/v1/counter/increment", map[string]any{"delta": 5})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("increment status: %d", resp.StatusCode)
	}
	inc := decode[testResponse](t, resp)
	if inc.CurrentValue != "5" {
		t.Fatalf("expected value 5, got %q", inc.CurrentValue)
	}

	resp = doJSON(t, http.MethodGet, srv.URL+"/v1/counter/value", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get value status: %d", resp.StatusCode)
	}
	val := decode[testResponse](t, resp)
	if val.CurrentValue != "5" {
		t.Fatalf("expected value 5, got %q", val.CurrentValue)
	}
}

func TestGatewayMultiCounterAndList(t *testing.T) {
	srv, _, _, _ := newTestGateway(t)

	resp := doJSON(t, http.MethodPost, srv.URL+"/v1/counter/increment", map[string]any{"delta": 2, "counterName": "post_1"})
	inc := decode[testResponse](t, resp)
	if inc.CurrentValue != "2" || inc.CounterName != "post_1" {
		t.Fatalf("unexpected multi-counter response: %+v", inc)
	}

	resp = doJSON(t, http.MethodGet, srv.URL+"/v1/counters", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status: %d", resp.StatusCode)
	}
	var list struct {
		Counters []struct {
			Name         string `json:"name"`
			CurrentValue string `json:"currentValue"`
		} `json:"counters"`
	}
	body := decode[struct {
		Counters []struct {
			Name         string `json:"name"`
			CurrentValue string `json:"currentValue"`
		} `json:"counters"`
	}](t, resp)
	list = body
	found := false
	for _, c := range list.Counters {
		if c.Name == "post_1" && c.CurrentValue == "2" {
			found = true
		}
	}
	if !found {
		t.Fatalf("post_1 counter not listed correctly: %+v", list.Counters)
	}
}

func TestGatewayAdminAddRemoveNode(t *testing.T) {
	srv, _, mem, _ := newTestGateway(t)

	resp := doJSON(t, http.MethodPost, srv.URL+"/v1/admin/add-node", map[string]any{"nodeId": "node-x", "address": "localhost:50099"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("add-node status: %d", resp.StatusCode)
	}
	if _, exists := mem.GetMember("node-x"); !exists {
		t.Fatal("node-x not added to membership")
	}

	resp = doJSON(t, http.MethodPost, srv.URL+"/v1/admin/remove-node", map[string]any{"nodeId": "node-x"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("remove-node status: %d", resp.StatusCode)
	}
	if m, exists := mem.GetMember("node-x"); exists && m.Status != cluster.StatusLeft {
		t.Fatalf("node-x not marked Left: %+v", m)
	}
}

func TestGatewayAdminForceSync(t *testing.T) {
	srv, svc, mem, eng := newTestGateway(t)

	_ = svc
	_ = mem
	resp := doJSON(t, http.MethodPost, srv.URL+"/v1/admin/force-sync", map[string]any{})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("force-sync status: %d", resp.StatusCode)
	}
	var fs struct {
		Success        bool  `json:"success"`
		Message        string `json:"message"`
		PeersContacted int32 `json:"peersContacted"`
	}
	out := decode[struct {
		Success        bool  `json:"success"`
		Message        string `json:"message"`
		PeersContacted int32 `json:"peersContacted"`
	}](t, resp)
	fs = out
	if !fs.Success {
		t.Fatalf("force-sync failed: %s", fs.Message)
	}
	// Peers exist but their servers are not running; contacting a peer is
	// expected (dial may fail silently). We only assert the RPC itself works.
	_ = eng
	_ = fs.PeersContacted
}
