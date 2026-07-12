package service

import (
	"context"
	"fmt"
	"time"
	"io"

	"go.uber.org/zap"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/crdt"
	"github.com/VeryFach/distributed-counter/internal/metrics"
)

type CounterService struct {
	pb.UnimplementedCounterServiceServer
	nodeID  string
	port    int
	counter *crdt.PNCounter
	clock   *crdt.VectorClock
	cluster *cluster.Membership
	logger  *zap.Logger
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

        // kirim acknowledgement
        ack := &pb.StateUpdate{
            FromNodeId: s.nodeID,
            Timestamp:  time.Now().Unix(),
            Type:       pb.StateUpdate_HEARTBEAT,
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
