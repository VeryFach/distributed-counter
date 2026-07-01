package gossip

import (
	"context"
	"sync"
	"time"

	"github.com/VeryFach/distributed-counter/api/proto"
	"go.uber.org/zap"
)

type GossipEngine struct {
	nodeID  string
	logger  *zap.Logger
	counter *crdt.PNCounter
	cluster *cluster.Membership

	// gRPC connections pool
	connections map[string]counter.CounterServiceClient
	streams     map[string]counter.CounterService_SyncStateClient

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

func NewGossipEngine(nodeID string, logger *zap.Logger) *GossipEngine {
	ctx, cancel := context.WithCancel(context.Background())
	return &GossipEngine{
		nodeID:      nodeID,
		logger:      logger,
		connections: make(map[string]counter.CounterServiceClient),
		streams:     make(map[string]counter.CounterService_SyncStateClient),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start starts the gossip protocol
func (g *GossipEngine) Start() {
	g.logger.Info("Starting gossip engine", zap.String("node_id", g.nodeID))

	// Periodic gossip with random peers
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.gossip()
		}
	}
}

// gossip performs a gossip round
func (g *GossipEngine) gossip() {
	peers := g.cluster.GetRandomPeers(3)
	if len(peers) == 0 {
		return
	}

	for _, peer := range peers {
		go g.gossipToPeer(peer)
	}
}

// gossipToPeer sends state update to a single peer
func (g *GossipEngine) gossipToPeer(peer *cluster.Member) {
	// Create state update message
	update := &proto.StateUpdate{
		FromNodeId:    g.nodeID,
		CounterValue:  g.counter.Value(),
		PositiveDelta: 0, // In real implementation, calculate delta since last sync
		NegativeDelta: 0,
		VectorClock:   g.getVectorClock(),
		Timestamp:     time.Now().Unix(),
		Type:          proto.StateUpdate_DELTA_UPDATE,
	}

	// Send via gRPC streaming
	stream, err := g.getOrCreateStream(peer.Address)
	if err != nil {
		g.logger.Error("Failed to create stream", zap.Error(err))
		return
	}

	if err := stream.Send(update); err != nil {
		g.logger.Error("Failed to send state update", zap.Error(err))
		return
	}

	// Receive response (bi-directional streaming)
	response, err := stream.Recv()
	if err != nil {
		g.logger.Error("Failed to receive response", zap.Error(err))
		return
	}

	// Merge received state
	g.counter.Merge(parseCounter(response.CounterValue))
	g.logger.Debug("State synchronized",
		zap.String("peer", peer.Address),
		zap.Int64("new_value", g.counter.Value()))
}
