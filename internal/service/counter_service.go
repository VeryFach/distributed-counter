package service

import (
	"context"
	"fmt"
	"time"

	//"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/crdt"
)

type CounterService struct {
	proto.UnimplementedCounterServiceServer
	nodeID  string
	counter *crdt.PNCounter
	clock   *crdt.VectorClock
	cluster *cluster.Membership
	logger  *zap.Logger
}

func NewCounterService(nodeID string, logger *zap.Logger) *CounterService {
	return &CounterService{
		nodeID:  nodeID,
		counter: crdt.NewPNCounter(nodeID),
		clock:   crdt.NewVectorClock(nodeID),
		logger:  logger,
	}
}

func (s *CounterService) Increment(ctx context.Context, req *proto.IncrementRequest) (*proto.CounterResponse, error) {
	s.logger.Debug("Increment called", zap.Int32("delta", req.Delta))

	delta := int64(req.Delta)
	if delta == 0 {
		delta = 1
	}

	s.counter.Increment(delta)
	s.clock.Increment()

	return s.buildResponse(), nil
}

func (s *CounterService) Decrement(ctx context.Context, req *proto.DecrementRequest) (*proto.CounterResponse, error) {
	s.logger.Debug("Decrement called", zap.Int32("delta", req.Delta))

	delta := int64(req.Delta)
	if delta == 0 {
		delta = 1
	}

	s.counter.Decrement(delta)
	s.clock.Increment()

	return s.buildResponse(), nil
}

func (s *CounterService) GetValue(ctx context.Context, req *proto.GetValueRequest) (*proto.CounterResponse, error) {
	return s.buildResponse(), nil
}

func (s *CounterService) GetNodeInfo(ctx context.Context, req *proto.GetNodeInfoRequest) (*proto.NodeInfo, error) {
	return &proto.NodeInfo{
		NodeId:       s.nodeID,
		Address:      fmt.Sprintf("localhost:%d", s.getPort()),
		CounterValue: s.counter.Value(),
		Version:      s.clock.String(),
		IsLeader:     false,
		LastSeen:     time.Now().Unix(),
	}, nil
}

func (s *CounterService) buildResponse() *proto.CounterResponse {
	nodes := []*proto.NodeInfo{}
	if s.cluster != nil {
		for _, member := range s.cluster.GetMembers() {
			nodes = append(nodes, &proto.NodeInfo{
				NodeId:       member.ID,
				Address:      member.Address,
				CounterValue: member.CounterValue,
				IsActive:     member.IsActive,
			})
		}
	}

	return &proto.CounterResponse{
		NodeId:       s.nodeID,
		CurrentValue: s.counter.Value(),
		Version:      s.clock.String(),
		LastUpdated:  time.Now().Unix(),
		ClusterNodes: nodes,
	}
}

// Helper method - implement port getter
func (s *CounterService) getPort() int {
	// In real implementation, get from config
	return 50051
}

// SetCluster injects cluster dependency
func (s *CounterService) SetCluster(cluster *cluster.Membership) {
	s.cluster = cluster
}
