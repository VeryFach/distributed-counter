package gossip

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	counter "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/crdt"
	"github.com/VeryFach/distributed-counter/internal/metrics"
	"github.com/VeryFach/distributed-counter/internal/persistence"
	"github.com/VeryFach/distributed-counter/pkg/grpcutil"
)

type GossipEngine struct {
	nodeID   string
	logger   *zap.Logger
	counter  *crdt.PNCounter
	cluster  *cluster.Membership
	clock    *crdt.VectorClock
	interval time.Duration

	// connections pool for re-dialing peers after failures
	connections map[string]*grpc.ClientConn

	// syncing tracks peers with an in-flight gossip round so concurrent
	// rounds never share a stream (and never pile up goroutines).
	syncing map[string]bool

	// lastSync tracks, per peer, the vector clock of the state we last
	// successfully sent. It is the "LastSyncVersion" baseline used to send
	// only delta (changed) entries instead of the full state every round.
	lastSync map[string]map[string]int64

	// breakers tracks per-peer circuit breakers so a dead node is not
	// hammered on every gossip round.
	breakers map[string]*CircuitBreaker

	// wal is the optional write-ahead log for durability of remote merges.
	wal *persistence.WALStore

	// client config for dialing peers (auth + compression).
	apiKey       string
	compression  bool

	// round counts gossip attempts; every fullSyncInterval rounds we send
	// the full state as a reconciliation safety net (a restarted peer may
	// have lost state that a stale delta baseline would otherwise skip).
	round            int
	fullSyncInterval int

	// retry/backoff tuning for gossip delivery.
	maxRetries      int
	backoffBase     time.Duration
	backoffMax      time.Duration
	circuitFailures int
	circuitCooldown time.Duration

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc

	// tracer emits a span per gossip round to a peer so the gossip loop is
	// visible in the trace backend.
	tracer trace.Tracer
}

func NewGossipEngine(
	nodeID string,
	pnCounter *crdt.PNCounter,
	clock *crdt.VectorClock,
	cluster *cluster.Membership,
	interval time.Duration,
	logger *zap.Logger,
) *GossipEngine {
	ctx, cancel := context.WithCancel(context.Background())

	if interval <= 0 {
		interval = 5 * time.Second
	}

	return &GossipEngine{
		nodeID:           nodeID,
		counter:          pnCounter,
		clock:            clock,
		cluster:          cluster,
		logger:           logger,
		interval:         interval,
		connections:      make(map[string]*grpc.ClientConn),
		syncing:          make(map[string]bool),
		lastSync:         make(map[string]map[string]int64),
		breakers:         make(map[string]*CircuitBreaker),
		fullSyncInterval: 10,
		maxRetries:       3,
		backoffBase:      250 * time.Millisecond,
		backoffMax:       2 * time.Second,
		circuitFailures:  3,
		circuitCooldown:  30 * time.Second,
		ctx:              ctx,
		cancel:           cancel,
		tracer:           otel.Tracer("gossip"),
	}
}

func (g *GossipEngine) breaker(address string) *CircuitBreaker {
	g.mu.Lock()
	defer g.mu.Unlock()

	if b, exists := g.breakers[address]; exists {
		return b
	}
	b := newCircuitBreaker(g.circuitFailures, g.circuitCooldown)
	g.breakers[address] = b
	return b
}

// SetWAL injects the write-ahead log so remote merges are durably logged.
func (g *GossipEngine) SetWAL(wal *persistence.WALStore) {
	g.wal = wal
}

// SetCircuitBreakerConfig tunes the per-peer circuit breaker that skips
// gossiping to unreachable peers. Exported so test harnesses can speed up
// recovery after a partitioned node rejoins the cluster.
func (g *GossipEngine) SetCircuitBreakerConfig(failures int, cooldown time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if failures > 0 {
		g.circuitFailures = failures
	}
	g.circuitCooldown = cooldown
}

// SetFullSyncInterval overrides how many gossip rounds pass before a full
// state reconciliation is sent as a safety net. Exported for tests that want
// deterministic full-sync behavior.
func (g *GossipEngine) SetFullSyncInterval(rounds int) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if rounds > 0 {
		g.fullSyncInterval = rounds
	}
}

// SetClientConfig configures how the engine dials peers: apiKey enables
// API key auth on the connection, compression enables gRPC gzip for the
// (potentially large) state payloads.
func (g *GossipEngine) SetClientConfig(apiKey string, compression bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.apiKey = apiKey
	g.compression = compression
}

// InvalidateBaselines clears the per-peer delta baselines. It must be called
// after a local Reset: the vector clock returns to zero while the old
// baseline would make delta gossip skip everything, leaving peers (which may
// also have been reset) permanently out of sync.
func (g *GossipEngine) InvalidateBaselines() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.lastSync = make(map[string]map[string]int64)
}

// backoff returns the exponential backoff duration for a given attempt.
func (g *GossipEngine) backoff(attempt int) time.Duration {
	d := g.backoffBase * time.Duration(1<<attempt)
	if d > g.backoffMax {
		d = g.backoffMax
	}
	return d
}

// Start starts the gossip protocol
func (g *GossipEngine) Start() {
	g.logger.Info("Starting gossip engine",
		zap.String("node_id", g.nodeID),
		zap.Duration("interval", g.interval),
	)

	// Periodic gossip with random peers
	ticker := time.NewTicker(g.interval)
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
		// Skip peers that already have an in-flight round so concurrent
		// rounds never share a gRPC stream (which can desync or nil-deref)
		// and goroutines cannot pile up on a slow/dead peer.
		g.mu.Lock()
		if g.syncing[peer.Address] {
			g.mu.Unlock()
			continue
		}
		g.syncing[peer.Address] = true
		g.mu.Unlock()

		go func(p *cluster.Member) {
			defer func() {
				g.mu.Lock()
				delete(g.syncing, p.Address)
				g.mu.Unlock()
			}()
			g.gossipToPeer(p)
		}(peer)
	}
}

// gossipToPeer sends a delta (or periodic full) state update to a single peer
func (g *GossipEngine) gossipToPeer(peer *cluster.Member) {
	ctx, span := g.tracer.Start(g.ctx, "gossip.sync",
		trace.WithAttributes(
			attribute.String("node.id", g.nodeID),
			attribute.String("peer.id", peer.ID),
			attribute.String("peer.address", peer.Address),
		),
	)
	defer span.End()

	breaker := g.breaker(peer.Address)
	if !breaker.Allow() {
		span.SetAttributes(attribute.Bool("breaker.open", true))
		return
	}

	myPos := g.counter.Positive()
	myNeg := g.counter.Negative()
	myClock := g.clock.State()

	g.mu.RLock()
	base := g.lastSync[peer.Address]
	g.mu.RUnlock()

	g.mu.Lock()
	g.round++
	// Send the full state when our delta baseline is stale. This happens
	// after a local Reset or restart cleared the vector clock while the
	// per-peer baseline kept its old (higher) version; a pure delta would
	// then skip everything and the peer would never catch up.
	staleBaseline := crdt.ClockNewerThan(base, myClock)
	fullSync := g.round%g.fullSyncInterval == 0 || staleBaseline
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
			Membership:      g.cluster.GossipMembership(),
		}
	} else {
		// Only send entries that changed since the last successful sync
		// with this peer. If nothing changed, skip to avoid resending
		// identical state.
		deltaPos, deltaNeg, deltaClock := crdt.DeltaFrom(myPos, myNeg, myClock, base)
		if len(deltaClock) == 0 {
			// Nothing changed for this peer since the last sync. Normally
			// this is expected, but if the baseline looks stale relative to
			// the peer's known state we log it so a wrong skip is visible.
			if crdt.ClockNewerThan(base, myClock) {
				g.logger.Warn("Delta skip with stale baseline",
					zap.String("peer", peer.Address),
					zap.Any("base", base),
					zap.Any("clock", myClock),
				)
			}
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
			Membership:      g.cluster.GossipMembership(),
		}
	}

	// Send via gRPC streaming with retry + exponential backoff. A fresh
	// stream is opened per round (backed by a cached connection); on failure
	// the connection is evicted so the next attempt re-dials the peer's
	// (possibly new) address. Otherwise a cached stream that died (e.g. peer
	// restart) would fail forever and the peer would never receive gossip.
	syncCtx, syncCancel := context.WithTimeout(ctx, g.interval)
	defer syncCancel()

	stream, err := g.getOrCreateStream(peer.Address, syncCtx)
	if err != nil {
		g.logger.Error("Failed to create stream", zap.Error(err))
		return
	}

	var response *counter.StateUpdate
	for attempt := 0; attempt < g.maxRetries; attempt++ {
		if err := stream.Send(update); err != nil {
			g.evictConn(peer.Address)
			if attempt < g.maxRetries-1 {
				time.Sleep(g.backoff(attempt))
				stream, err = g.getOrCreateStream(peer.Address, syncCtx)
				if err != nil {
					continue
				}
			}
			continue
		}

		// Receive response (bi-directional streaming)
		response, err = stream.Recv()
		if err == nil {
			break
		}

		g.evictConn(peer.Address)
		if attempt < g.maxRetries-1 {
			time.Sleep(g.backoff(attempt))
			stream, err = g.getOrCreateStream(peer.Address, syncCtx)
			if err != nil {
				continue
			}
		}
	}

	if response == nil {
		breaker.Failure()
		span.SetStatus(codes.Error, "gossip sync failed")
		g.logger.Error("Failed to sync state with peer",
			zap.String("peer", peer.Address),
			zap.Error(err),
		)
		return
	}

	breaker.Success()
	span.SetAttributes(attribute.Bool("synced", true))
	metrics.IncGossipSent(g.nodeID)

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

	if len(update.Membership) > 0 {
		g.cluster.ApplyMembership(update.Membership)
	}

	if g.wal != nil {
		if err := g.wal.Append(g.nodeID, "merge", 0, update.PositiveState, update.NegativeState, update.VectorClock); err != nil {
			g.logger.Error("Failed to append WAL entry for remote merge", zap.Error(err))
		}
	}
}

// getOrCreateStream returns a FRESH gRPC stream to the peer, backed by a
// cached *grpc.ClientConn. Streams are never shared between rounds or reused
// after an error: a bidirectional stream is only safe for a single
// concurrent send/receive owner. The connection itself is evicted on failure
// (see evictConn) so the next round re-resolves the peer address.
func (g *GossipEngine) getOrCreateStream(address string, ctx context.Context) (counter.CounterService_SyncStateClient, error) {
	g.mu.Lock()
	conn, exists := g.connections[address]
	if !exists {
		var err error
		conn, err = grpc.Dial(address, grpcutil.DialOptions(g.apiKey, g.compression)...)
		if err != nil {
			g.mu.Unlock()
			return nil, err
		}
		g.connections[address] = conn
	}
	g.mu.Unlock()

	stream, err := counter.NewCounterServiceClient(conn).SyncState(ctx)
	if err != nil || stream == nil {
		if err == nil {
			err = errors.New("gossip: SyncState returned a nil stream")
		}
		return nil, err
	}
	return stream, nil
}

// evictConn closes and removes the cached connection for a peer after a
// send or receive failure, so the next attempt starts fresh instead of
// reusing a dead connection forever.
func (g *GossipEngine) evictConn(address string) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if conn, exists := g.connections[address]; exists {
		_ = conn.Close()
		delete(g.connections, address)
	}
}

// Stop gracefully stops the gossip engine
func (g *GossipEngine) Stop() {
	g.cancel()
}
