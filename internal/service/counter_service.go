package service

import (
	"context"
	"fmt"
	"io"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/peer"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/crdt"
	"github.com/VeryFach/distributed-counter/internal/metrics"
)

type CounterService struct {
	pb.UnimplementedCounterServiceServer
	nodeID   string
	port     int
	counter  *crdt.PNCounter
	clock    *crdt.VectorClock
	cluster  *cluster.Membership
	logger   *zap.Logger
	onUpdate func(*pb.StateUpdate)
}

func NewCounterService(nodeID string, port int, logger *zap.Logger) *CounterService {
	return &CounterService{
		nodeID:  nodeID,
		port:    port,
		counter: crdt.NewPNCounter(nodeID),
		clock:   crdt.NewVectorClock(nodeID),
		logger:  logger,
	}
}

func (s *CounterService) getPort() int {
	return s.port
}

func (s *CounterService) Increment(ctx context.Context, req *pb.IncrementRequest) (*pb.CounterResponse, error) {
	s.logger.Debug("Increment called", zap.Int32("delta", req.Delta))

	delta := int64(req.Delta)
	if delta == 0 {
		delta = 1
	}

	s.counter.Increment(delta)
	s.clock.Increment()

	metrics.IncIncrementTotal(s.nodeID)
	metrics.UpdateCounterValue(s.nodeID, s.counter.Value())

	return s.buildResponse(), nil
}

func (s *CounterService) Decrement(ctx context.Context, req *pb.DecrementRequest) (*pb.CounterResponse, error) {
	s.logger.Debug("Decrement called", zap.Int32("delta", req.Delta))

	delta := int64(req.Delta)
	if delta == 0 {
		delta = 1
	}

	s.counter.Decrement(delta)
	s.clock.Increment()

	metrics.IncDecrementTotal(s.nodeID)
	metrics.UpdateCounterValue(s.nodeID, s.counter.Value())

	return s.buildResponse(), nil
}

func (s *CounterService) GetValue(ctx context.Context, req *pb.GetValueRequest) (*pb.CounterResponse, error) {
	return s.buildResponse(), nil
}

func (s *CounterService) GetNodeInfo(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.NodeInfo, error) {
	return &pb.NodeInfo{
		NodeId:       s.nodeID,
		Address:      fmt.Sprintf("localhost:%d", s.getPort()),
		CounterValue: s.counter.Value(),
		Version:      s.clock.String(),
		IsLeader:     false,
		LastSeen:     time.Now().Unix(),
	}, nil
}

func (s *CounterService) JoinCluster(
	req *pb.JoinRequest,
	stream pb.CounterService_JoinClusterServer,
) error {
	if s.cluster != nil {
		s.cluster.AddMember(req.NodeId, req.Address)
	}

	members := []*pb.Member{}
	if s.cluster != nil {
		for _, member := range s.cluster.GetMembers() {
			members = append(members, &pb.Member{
				NodeId:        member.ID,
				Address:       member.Address,
				IsActive:      member.IsActive,
				LastHeartbeat: member.LastHeartbeat.Unix(),
				CounterValue:  member.CounterValue,
			})
		}
	}

	if len(members) == 0 && req.NodeId != "" {
		members = append(members, &pb.Member{
			NodeId:        req.NodeId,
			Address:       req.Address,
			IsActive:      true,
			LastHeartbeat: time.Now().Unix(),
		})
	}

	return stream.Send(&pb.MemberList{
		Members: members,
		Version: time.Now().Unix(),
	})
}

func (s *CounterService) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if s.cluster != nil {
		if _, exists := s.cluster.GetMember(req.NodeId); exists {
			s.cluster.UpdateHeartbeat(req.NodeId)
		} else {
			remoteAddress := ""
			if remotePeer, ok := peer.FromContext(ctx); ok && remotePeer.Addr != nil {
				remoteAddress = remotePeer.Addr.String()
			}
			s.cluster.AddOrUpdateMember(req.NodeId, remoteAddress, true, time.Unix(req.Timestamp, 0))
		}
	}

	activeMembers := make([]*pb.Member, 0)
	clusterSize := int64(0)
	if s.cluster != nil {
		for _, member := range s.cluster.GetMembers() {
			if member.IsActive {
				activeMembers = append(activeMembers, &pb.Member{
					NodeId:        member.ID,
					Address:       member.Address,
					IsActive:      member.IsActive,
					LastHeartbeat: member.LastHeartbeat.Unix(),
					CounterValue:  member.CounterValue,
				})
			}
		}
		clusterSize = int64(len(activeMembers))
	}

	return &pb.HeartbeatResponse{
		Success:       true,
		Message:       "heartbeat acknowledged",
		ClusterSize:   clusterSize,
		ActiveMembers: activeMembers,
	}, nil
}

func (s *CounterService) buildResponse() *pb.CounterResponse {
	nodes := []*pb.NodeInfo{}
	if s.cluster != nil {
		for _, member := range s.cluster.GetMembers() {
			nodes = append(nodes, &pb.NodeInfo{
				NodeId:       member.ID,
				Address:      member.Address,
				CounterValue: member.CounterValue,
				LastSeen:     member.LastHeartbeat.Unix(),
			})
		}
	}

	return &pb.CounterResponse{
		NodeId:       s.nodeID,
		CurrentValue: s.counter.Value(),
		Version:      s.clock.String(),
		LastUpdated:  time.Now().Unix(),
		ClusterNodes: nodes,
	}
}

// SetCluster injects cluster dependency
func (s *CounterService) SetCluster(cluster *cluster.Membership) {
	s.cluster = cluster
}

func (s *CounterService) SyncState(
	stream pb.CounterService_SyncStateServer,
) error {
	for {
		select {
		case <-stream.Context().Done():
			s.logger.Debug("sync stream closed")
			return nil
		default:
		}

		update, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			s.logger.Error("sync receive failed",
				zap.Error(err))
			return err
		}

		// Jangan ACK heartbeat lagi
		if update.Type == pb.StateUpdate_HEARTBEAT {
			continue
		}

		remote := crdt.NewPNCounter("")
		remote.SetPositive(update.PositiveState)
		remote.SetNegative(update.NegativeState)

		s.counter.Merge(remote)
		s.clock.MergeMap(update.VectorClock)

		metrics.UpdateCounterValue(
			s.nodeID,
			s.counter.Value(),
		)
		metrics.IncGossipReceived(s.nodeID)

		// balas dengan full state supaya peer bisa merge atau recovery
		ack := &pb.StateUpdate{
			FromNodeId:    s.nodeID,
			PositiveState: s.counter.Positive(),
			NegativeState: s.counter.Negative(),
			VectorClock:   s.clock.State(),
			Timestamp:     time.Now().Unix(),
			Type:          pb.StateUpdate_FULL_STATE,
		}

		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

func (s *CounterService) Counter() *crdt.PNCounter {
	return s.counter
}

func (s *CounterService) Clock() *crdt.VectorClock {
	return s.clock
}

func (s *CounterService) Cluster() *cluster.Membership {
	return s.cluster
}
