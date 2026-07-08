package gossip

import (
	"io"

	"go.uber.org/zap"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/crdt"
)

type GossipHandler struct{
	pb.UnimplementedCounterServiceServer
	nodeID	string
	counter	*crdt.PNCounter
	logger *zap.Logger
}

func NewGossipHandler (nodeID string, counter *crdt.PNCounter, logger *zap.Logger) *GossipHandler {
	return &GossipHandler{
		nodeID: nodeID,
		counter: counter,
		logger: logger,
	}
}

func (h *GossipHandler) SyncState(stream pb.CounterService_SyncStateServer) error {
	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			update,err := stream.Recv()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				h.logger.Error("Failed to recive state update", zap.Error(err))
				return err
			}
			// Merge recived state into local counter
			h.logger.Debug("Recived state update", zap.String("from", update.FromNodeId), zap.Int64("value", update.CounterValue))
			// In real implementation, merge CRDT state (positive/negative deltas)
            // For now, we just update counter (simplistic)
            // You should merge using PNCounter.Merge() from other node's full state.
            // We'll need to send full state, not just delta.
            // For simplicity, we'll just log.
		}
	}
}