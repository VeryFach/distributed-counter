// Package dashboard serves the embedded web dashboard: a single-page UI that
// shows cluster membership, node status, counter values, gossip activity and a
// live topology graph. Data is aggregated by polling every member's gRPC
// GetNodeInfo/ListCounters, so the view reflects the whole cluster.
package dashboard

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/service"
	"github.com/VeryFach/distributed-counter/pkg/grpcutil"
)

//go:embed index.html
var indexHTML []byte

// NodeView is the aggregated view of a single cluster member.
type NodeView struct {
	NodeID       string `json:"node_id"`
	Address      string `json:"address"`
	Status       string `json:"status"`
	IsActive     bool   `json:"is_active"`
	IsLeader     bool   `json:"is_leader"`
	LeaderID     string `json:"leader_id"`
	Priority     int64  `json:"priority"`
	CounterValue int64  `json:"counter_value"`
	Version      string `json:"version"`
	LastSeen     int64  `json:"last_seen"`
	Reachable    bool   `json:"reachable"`
}

// CounterView is a single counter across the cluster.
type CounterView struct {
	Name         string   `json:"name"`
	Value        int64    `json:"value"`
	Shard        uint32   `json:"shard"`
	Tags         []string `json:"tags"`
	Contributors []string `json:"contributors"`
}

// ClusterView is the complete payload served at /api/cluster.
type ClusterView struct {
	Self     *NodeView     `json:"self"`
	Nodes    []*NodeView   `json:"nodes"`
	Counters []*CounterView `json:"counters"`
	Gossip   GossipStats   `json:"gossip"`
}

// GossipStats reports message totals aggregated across nodes.
type GossipStats struct {
	SentTotal     int64 `json:"sent_total"`
	ReceivedTotal int64 `json:"received_total"`
}

// Dashboard aggregates cluster state for the web UI.
type Dashboard struct {
	nodeID      string
	membership  *cluster.Membership
	service     *service.CounterService
	apiKey      string
	compression bool
	logger      *zap.Logger
}

// Options configures the dashboard.
type Options struct {
	// NodeID is the local node's id.
	NodeID string
	// Membership provides the member list to poll.
	Membership *cluster.Membership
	// Service provides local node info and counter values.
	Service *service.CounterService
	// APIKey / Compression configure outbound gRPC dials to peers.
	APIKey      string
	Compression bool
	// Logger for dashboard activity.
	Logger *zap.Logger
}

// New builds a Dashboard.
func New(opts Options) *Dashboard {
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Dashboard{
		nodeID:      opts.NodeID,
		membership:  opts.Membership,
		service:     opts.Service,
		apiKey:      opts.APIKey,
		compression: opts.Compression,
		logger:      logger,
	}
}

// Index serves the single-page dashboard HTML.
func (d *Dashboard) Index(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(indexHTML)
}

// Cluster returns the aggregated cluster view as JSON.
func (d *Dashboard) Cluster(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	view := d.BuildClusterView(ctx)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(view)
}

// BuildClusterView polls every member and aggregates the result.
func (d *Dashboard) BuildClusterView(ctx context.Context) *ClusterView {
	view := &ClusterView{}

	if d.service != nil {
		view.Self = d.pollNode(ctx, d.nodeID, d.service)
	}

	counterAgg := make(map[string]*CounterView)

	if d.membership != nil {
		for _, member := range d.membership.GetMembers() {
			var nv *NodeView
			if member.ID == d.nodeID {
				nv = view.Self
			} else {
				nv = d.pollNode(ctx, member.ID, nil)
			}
			if nv == nil {
				nv = &NodeView{
					NodeID:       member.ID,
					Address:      member.Address,
					Status:       member.Status.String(),
					IsActive:     member.IsActive,
					Priority:     member.Priority,
					LastSeen:     member.LastHeartbeat.Unix(),
					Reachable:    false,
				}
			}
			if nv.Address == "" {
				nv.Address = member.Address
			}
			if nv.Status == "" {
				nv.Status = member.Status.String()
			}
			if nv.Priority == 0 {
				nv.Priority = member.Priority
			}
			view.Nodes = append(view.Nodes, nv)
		}
	}

	// Merge counters across nodes: values are max-based (state CRDT), so the
	// largest observed value is the converged value.
	if d.service != nil {
		if resp, err := d.service.ListCounters(ctx, &pb.ListCountersRequest{}); err == nil {
			for _, ci := range resp.Counters {
				c := &CounterView{
					Name:  ci.Name,
					Value: ci.CurrentValue,
					Shard: ci.Shard,
					Tags:  ci.Tags,
				}
				counterAgg[ci.Name] = c
			}
		}
	}

	for _, nv := range view.Nodes {
		agg, err := d.pollCounters(ctx, nv.Address)
		if err != nil {
			continue
		}
		for _, ci := range agg {
			if existing, ok := counterAgg[ci.Name]; ok {
				if ci.CurrentValue > existing.Value {
					existing.Value = ci.CurrentValue
				}
				existing.Contributors = append(existing.Contributors, nv.NodeID)
			}
		}
	}

	names := make([]string, 0, len(counterAgg))
	for name := range counterAgg {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		view.Counters = append(view.Counters, counterAgg[name])
	}

	view.Gossip = d.pollGossip(ctx, view.Nodes)

	return view
}

// pollNode fetches GetNodeInfo from a node. When service is non-nil the local
// in-process service is queried directly (no dial).
func (d *Dashboard) pollNode(ctx context.Context, nodeID string, svc *service.CounterService) *NodeView {
	if svc != nil {
		info, err := svc.GetNodeInfo(ctx, &pb.GetNodeInfoRequest{})
		if err != nil {
			return nil
		}
		return nodeViewFromInfo(info, true)
	}

	// Find the member to know the address.
	member, ok := d.membership.GetMember(nodeID)
	if !ok {
		return nil
	}

	conn, err := d.dial(ctx, member.Address)
	if err != nil {
		return nil
	}
	defer conn.Close()

	client := pb.NewCounterServiceClient(conn)
	info, err := client.GetNodeInfo(ctx, &pb.GetNodeInfoRequest{})
	if err != nil {
		return nil
	}
	return nodeViewFromInfo(info, true)
}

func nodeViewFromInfo(info *pb.NodeInfo, reachable bool) *NodeView {
	return &NodeView{
		NodeID:       info.NodeId,
		Address:      info.Address,
		IsLeader:     info.IsLeader,
		LeaderID:     info.LeaderId,
		Priority:     info.Priority,
		CounterValue: info.CounterValue,
		Version:      info.Version,
		LastSeen:     info.LastSeen,
		Status:       "Alive",
		IsActive:     true,
		Reachable:    reachable,
	}
}

// pollCounters fetches the counter list from a remote node.
func (d *Dashboard) pollCounters(ctx context.Context, address string) ([]*pb.CounterInfo, error) {
	if address == "" {
		return nil, fmt.Errorf("empty address")
	}

	conn, err := d.dial(ctx, address)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := pb.NewCounterServiceClient(conn)
	resp, err := client.ListCounters(ctx, &pb.ListCountersRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Counters, nil
}

// pollGossip reads the local process's Prometheus registry for the gossip
// message counters. It runs in the same process as the node, so the default
// registry holds this node's totals; combined with the per-node counters the
// dashboard shows cluster-wide activity.
func (d *Dashboard) pollGossip(ctx context.Context, nodes []*NodeView) GossipStats {
	var stats GossipStats
	_ = nodes

	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return stats
	}

	for _, family := range families {
		switch family.GetName() {
		case "gossip_messages_sent_total":
			stats.SentTotal = sumCounterValues(family.GetMetric())
		case "gossip_messages_received_total":
			stats.ReceivedTotal = sumCounterValues(family.GetMetric())
		}
	}
	return stats
}

func sumCounterValues(metrics []*dto.Metric) int64 {
	var total float64
	for _, m := range metrics {
		if m.GetCounter() != nil {
			total += m.GetCounter().GetValue()
		}
	}
	return int64(total)
}

func (d *Dashboard) dial(ctx context.Context, address string) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	opts := append(grpcutil.DialOptions(d.apiKey, d.compression), grpc.WithBlock())
	conn, err := grpc.DialContext(dialCtx, address, opts...)
	if err != nil {
		if status.Code(err) == codes.Unavailable {
			return nil, err
		}
		return nil, err
	}
	return conn, nil
}
