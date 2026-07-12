package gossip

import (
	"context"
	"sync"
	"time"

	counter "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/crdt"
	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GossipEngine struct {
	nodeID  string
	logger  *zap.Logger
	counter *crdt.PNCounter
	cluster *cluster.Membership
	clock   *crdt.VectorClock

	// gRPC connections pool
	connections map[string]counter.CounterServiceClient
	streams     map[string]counter.CounterService_SyncStateClient

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
}

func NewGossipEngine(
	nodeID string,
	pnCounter *crdt.PNCounter,
	clock *crdt.VectorClock,
	cluster *cluster.Membership,
	logger *zap.Logger,
) *GossipEngine {
	ctx, cancel := context.WithCancel(context.Background())

	return &GossipEngine{
		nodeID:      nodeID,
		counter:     pnCounter,
		clock:       clock,
		cluster:     cluster,
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
	update := &counter.StateUpdate{
		FromNodeId:    g.nodeID,
		PositiveState: g.counter.Positive(),
		NegativeState: g.counter.Negative(),
		VectorClock:   g.clock.State(),
		Timestamp:     time.Now().Unix(),
		Type:          counter.StateUpdate_FULL_STATE,
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
	remote := crdt.NewPNCounter("")

	remote.SetPositive(response.PositiveState)
	remote.SetNegative(response.NegativeState)

	g.counter.Merge(remote)
	g.clock.MergeMap(response.VectorClock)
	g.logger.Debug("State synchronized",
		zap.String("peer", peer.Address),
		zap.Int64("new_value", g.counter.Value()))
}

// getOrCreateStream creates or returns existing gRPC stream to peer
func (g *GossipEngine) getOrCreateStream(address string) (counter.CounterService_SyncStateClient, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if stream, exists := g.streams[address]; exists {
		return stream, nil
	}

	conn, err := grpc.Dial(
		address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return nil, err
	}

	client := counter.NewCounterServiceClient(conn)

	stream, err := client.SyncState(g.ctx)
	if err != nil {
		return nil, err
	}

	g.connections[address] = client
	g.streams[address] = stream

	return stream, nil
}

// Stop gracefully stops the gossip engine
func (g *GossipEngine) Stop() {
	g.cancel()
}
