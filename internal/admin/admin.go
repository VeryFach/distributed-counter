// Package admin implements the AdminService RPCs: cluster-management
// operations (add/remove node, force sync) exposed over both gRPC and HTTP
// (via grpc-gateway), used by the Admin API and the web dashboard.
package admin

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/gossip"
)

// AdminService implements pb.AdminServiceServer.
type AdminService struct {
	pb.UnimplementedAdminServiceServer

	nodeID     string
	membership *cluster.Membership
	gossip     *gossip.GossipEngine
	logger     *zap.Logger
}

// New creates an AdminService for the given node.
func New(nodeID string, logger *zap.Logger) *AdminService {
	return &AdminService{
		nodeID: nodeID,
		logger: logger,
	}
}

// SetCluster injects the membership used to add/remove nodes.
func (a *AdminService) SetCluster(m *cluster.Membership) {
	a.membership = m
}

// SetGossip injects the gossip engine used by ForceSync.
func (a *AdminService) SetGossip(g *gossip.GossipEngine) {
	a.gossip = g
}

// AddNode registers a new member (or refreshes an existing one). Returns the
// resulting member list. The node itself is added with the address supplied
// by the operator; a live node will subsequently report its real address via
// the heartbeat protocol.
func (a *AdminService) AddNode(ctx context.Context, req *pb.AddNodeRequest) (*pb.AdminResponse, error) {
	if req.NodeId == "" {
		return nil, fmt.Errorf("node_id is required")
	}
	if req.Address == "" {
		return nil, fmt.Errorf("address is required")
	}

	if a.membership != nil {
		a.membership.AddOrUpdateMember(req.NodeId, req.Address, true, time.Now())
	}

	a.logger.Info("Admin: node added",
		zap.String("node_id", req.NodeId),
		zap.String("address", req.Address),
	)
	return a.response(fmt.Sprintf("node %q added", req.NodeId)), nil
}

// RemoveNode marks a member as Left so it is excluded from gossip and
// election. Returns the resulting member list.
func (a *AdminService) RemoveNode(ctx context.Context, req *pb.RemoveNodeRequest) (*pb.AdminResponse, error) {
	if req.NodeId == "" {
		return nil, fmt.Errorf("node_id is required")
	}
	if req.NodeId == a.nodeID {
		return nil, fmt.Errorf("cannot remove the local node")
	}

	if a.membership != nil {
		a.membership.MarkLeft(req.NodeId)
	}

	a.logger.Info("Admin: node removed",
		zap.String("node_id", req.NodeId),
	)
	return a.response(fmt.Sprintf("node %q removed", req.NodeId)), nil
}

// ForceSync triggers an immediate gossip round to every active peer, useful
// to converge faster after a network partition heals or to manually
// reconcile state.
func (a *AdminService) ForceSync(ctx context.Context, req *pb.ForceSyncRequest) (*pb.ForceSyncResponse, error) {
	if a.gossip == nil {
		return &pb.ForceSyncResponse{Success: false, Message: "gossip engine not wired"}, nil
	}

	contacted := a.gossip.ForceSync()
	return &pb.ForceSyncResponse{
		Success:        true,
		Message:        "force sync triggered",
		PeersContacted: int32(contacted),
	}, nil
}

// response assembles an AdminResponse with the current member list.
func (a *AdminService) response(message string) *pb.AdminResponse {
	res := &pb.AdminResponse{
		Success: true,
		Message: message,
	}

	if a.membership != nil {
		for _, member := range a.membership.GetMembers() {
			res.Members = append(res.Members, &pb.Member{
				NodeId:        member.ID,
				Address:       member.Address,
				IsActive:      member.IsActive,
				LastHeartbeat: member.LastHeartbeat.Unix(),
				CounterValue:  member.CounterValue,
				Status:        member.Status.ToProto(),
				Priority:      member.Priority,
			})
		}
	}

	return res
}
