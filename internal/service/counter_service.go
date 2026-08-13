package service

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/peer"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/crdt"
	"github.com/VeryFach/distributed-counter/internal/metrics"
	"github.com/VeryFach/distributed-counter/internal/persistence"
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
	store    persistence.Store

	// lastSyncClock tracks, per sender, the vector clock we last replied
	// with, so delta replies never resend identical state.
	mu            sync.Mutex
	lastSyncClock map[string]map[string]int64
}

func NewCounterService(nodeID string, port int, logger *zap.Logger) *CounterService {
	return &CounterService{
		nodeID:        nodeID,
		port:          port,
		counter:       crdt.NewPNCounter(nodeID),
		clock:         crdt.NewVectorClock(nodeID),
		logger:        logger,
		lastSyncClock: make(map[string]map[string]int64),
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

	s.persist()

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

	s.persist()

	return s.buildResponse(), nil
}

func (s *CounterService) Reset(ctx context.Context, req *pb.ResetRequest) (*pb.CounterResponse, error) {
	s.logger.Info("Reset called", zap.String("node_id", s.nodeID))

	s.counter.Reset()
	s.clock.Reset()

	metrics.UpdateCounterValue(s.nodeID, s.counter.Value())

	s.persist()

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

		// Merge incoming state. Delta updates only carry changed entries;
		// full state carries everything. Both are safe to apply because
		// PNCounter and vector clock merges are max-based.
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

		s.persist()

		// Balas dengan delta (atau full state) supaya peer bisa merge.
		var ack *pb.StateUpdate
		if update.Type == pb.StateUpdate_DELTA_UPDATE {
			ack = s.buildDeltaReply(update)
		} else {
			ack = s.buildFullReply()
		}

		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

// buildDeltaReply replies to a delta update with only the entries this node
// has that the sender does not yet know about. The base is the merge of the
// sender's clock and the last clock we already replied to that sender, so we
// never resend identical state (Versioned State / LastSyncVersion).
func (s *CounterService) buildDeltaReply(update *pb.StateUpdate) *pb.StateUpdate {
	myClock := s.clock.State()

	s.mu.Lock()
	base := crdt.MergeClock(update.VectorClock, s.lastSyncClock[update.FromNodeId])
	s.lastSyncClock[update.FromNodeId] = crdt.MergeClock(myClock, update.VectorClock)
	s.mu.Unlock()

	deltaPos, deltaNeg, _ := crdt.DeltaFrom(
		s.counter.Positive(),
		s.counter.Negative(),
		myClock,
		base,
	)

	return &pb.StateUpdate{
		FromNodeId:      s.nodeID,
		PositiveState:   deltaPos,
		NegativeState:   deltaNeg,
		VectorClock:     myClock,
		Timestamp:       time.Now().Unix(),
		Type:            pb.StateUpdate_DELTA_UPDATE,
		LastSyncVersion: crdt.MaxClock(base),
	}
}

func (s *CounterService) buildFullReply() *pb.StateUpdate {
	myClock := s.clock.State()

	return &pb.StateUpdate{
		FromNodeId:      s.nodeID,
		PositiveState:   s.counter.Positive(),
		NegativeState:   s.counter.Negative(),
		VectorClock:     myClock,
		Timestamp:       time.Now().Unix(),
		Type:            pb.StateUpdate_FULL_STATE,
		LastSyncVersion: crdt.MaxClock(myClock),
	}
}

// persist writes the current CRDT state to the configured store.
func (s *CounterService) persist() {
	if s.store == nil {
		return
	}

	state := persistence.CounterState{
		Positive: s.counter.Positive(),
		Negative: s.counter.Negative(),
		Clock:    s.clock.State(),
	}

	if err := s.store.Save(s.nodeID, state); err != nil {
		s.logger.Error("Failed to persist counter state", zap.Error(err))
	}
}

// Restore merges previously persisted state back into the local CRDT after
// a restart, so the counter survives node restarts.
func (s *CounterService) Restore(state *persistence.CounterState) {
	if state == nil {
		return
	}

	remote := crdt.NewPNCounter("")
	remote.SetPositive(state.Positive)
	remote.SetNegative(state.Negative)

	s.counter.Merge(remote)
	s.clock.MergeMap(state.Clock)

	metrics.UpdateCounterValue(s.nodeID, s.counter.Value())

	s.logger.Info("Restored counter state from persistence",
		zap.Int64("value", s.counter.Value()))
}

// SetStore injects the persistence store used to survive restarts.
func (s *CounterService) SetStore(store persistence.Store) {
	s.store = store
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
