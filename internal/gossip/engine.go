package gossip

import (
	"context"
	"sync"
	"time"

	counter "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/crdt"
	"github.com/VeryFach/distributed-counter/internal/metrics"
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

	// lastSync tracks, per peer, the vector clock of the state we last
	// successfully sent. It is the "LastSyncVersion" baseline used to send
	// only delta (changed) entries instead of the full state every round.
	lastSync map[string]map[string]int64

	// round counts gossip attempts; every fullSyncInterval rounds we send
	// the full state as a reconciliation safety net (a restarted peer may
	// have lost state that a stale delta baseline would otherwise skip).
	round            int
	fullSyncInterval int

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
		nodeID:           nodeID,
		counter:          pnCounter,
		clock:            clock,
		cluster:          cluster,
		logger:           logger,
		connections:      make(map[string]counter.CounterServiceClient),
		streams:          make(map[string]counter.CounterService_SyncStateClient),
		lastSync:         make(map[string]map[string]int64),
		fullSyncInterval: 10,
		ctx:              ctx,
		cancel:           cancel,
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

// gossipToPeer sends a delta (or periodic full) state update to a single peer
func (g *GossipEngine) gossipToPeer(peer *cluster.Member) {
	myPos := g.counter.Positive()
	myNeg := g.counter.Negative()
	myClock := g.clock.State()

	g.mu.RLock()
	base := g.lastSync[peer.Address]
	g.mu.RUnlock()

	g.mu.Lock()
	g.round++
	fullSync := g.round%g.fullSyncInterval == 0
	g.mu.Unlock()

	var update *counter.StateUpdate
	if fullSync {
		// Reconciliation pass: send the full state so a peer that lost
		// state (e.g. restarted without persistence) is fully caught up
		// even if our delta baseline is stale.
		update = &counter.StateUpdate{
			FromNodeId:      g.nodeID,
			PositiveState:   myPos,
			NegativeState:   myNeg,
			VectorClock:     myClock,
			Timestamp:       time.Now().Unix(),
			Type:            counter.StateUpdate_FULL_STATE,
			LastSyncVersion: crdt.MaxClock(myClock),
		}
	} else {
		// Only send entries that changed since the last successful sync
		// with this peer. If nothing changed, skip to avoid resending
		// identical state.
		deltaPos, deltaNeg, deltaClock := crdt.DeltaFrom(myPos, myNeg, myClock, base)
		if len(deltaClock) == 0 {
			return
		}

		update = &counter.StateUpdate{
			FromNodeId:      g.nodeID,
			PositiveState:   deltaPos,
			NegativeState:   deltaNeg,
			VectorClock:     myClock,
			Timestamp:       time.Now().Unix(),
			Type:            counter.StateUpdate_DELTA_UPDATE,
			LastSyncVersion: crdt.MaxClock(base),
		}
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
	metrics.IncGossipSent(g.nodeID)

	// Receive response (bi-directional streaming)
	response, err := stream.Recv()
	if err != nil {
		g.logger.Error("Failed to receive response", zap.Error(err))
		return
	}

	// Merge received state
	g.mergeRemoteState(response)

	// Record the state this peer has now seen so the next round only
	// carries changes (Versioned State / LastSyncVersion).
	g.mu.Lock()
	g.lastSync[peer.Address] = crdt.MergeClock(myClock, response.VectorClock)
	g.mu.Unlock()

	g.logger.Debug("State synchronized",
		zap.String("peer", peer.Address),
		zap.Bool("full_sync", fullSync),
		zap.Int64("new_value", g.counter.Value()))
}

// mergeRemoteState merges either a delta or full state update into the
// local CRDT. PNCounter merge is max-based, so deltas are safe to apply.
func (g *GossipEngine) mergeRemoteState(update *counter.StateUpdate) {
	remote := crdt.NewPNCounter("")
	remote.SetPositive(update.PositiveState)
	remote.SetNegative(update.NegativeState)

	g.counter.Merge(remote)
	g.clock.MergeMap(update.VectorClock)
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
