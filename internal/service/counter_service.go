package service

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/crdt"
)

type CounterService struct {
	pb.UnimplementedCounterServiceServer
	nodeID  string
	port    int
	counter *crdt.PNCounter
	clock   *crdt.VectorClock
	cluster *cluster.Membership
	logger  *zap.Logger
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
