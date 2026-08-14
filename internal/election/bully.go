// Package election implements leader election using the Bully algorithm.
// Every node carries a fixed priority; the live node with the highest
// priority becomes the coordinator. Nodes detect a stale leader via the
// periodic Run loop and start an election: they ask every higher-priority
// live peer to run for leadership, and only claim the role themselves when
// none of those peers respond. The winner announces itself by broadcasting
// a Coordinator message to all members.
//
// The elected leader is exposed through CounterService GetNodeInfo and is
// used for cluster management tasks such as coordinating snapshots.
package election

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/pkg/grpcutil"
)

// Bully implements the Bully leader election algorithm.
type Bully struct {
	nodeID     string
	priority   int64
	membership *cluster.Membership
	logger     *zap.Logger

	apiKey      string
	compression bool

	// interval is how often Run checks the leadership health; timeout is how
	// long a leader may go silent before an election is triggered.
	interval time.Duration
	timeout  time.Duration

	// dialTimeout bounds each Election/Coordinator RPC and probe retries.
	dialTimeout time.Duration

	// maxProbes is how many times an election probe is retried before a
	// higher-priority peer is considered unreachable, guarding against a
	// transient failure wrongly handing leadership to a lower-priority node.
	maxProbes int

	// leader state.
	leaderID        string
	leaderPriority  int64
	leaderTerm      int64
	lastLeaderSeen  time.Time
	electionRunning bool

	mu sync.Mutex

	tracer trace.Tracer
}

// Config builds a Bully instance.
type Config struct {
	NodeID        string
	Priority      int64
	Membership    *cluster.Membership
	Logger        *zap.Logger
	APIKey        string
	Compression   bool
	Interval      time.Duration
	LeaderTimeout time.Duration
}

// New creates a Bully election instance.
func New(cfg Config) *Bully {
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 2 * time.Second
	}
	if cfg.LeaderTimeout <= 0 {
		cfg.LeaderTimeout = 10 * time.Second
	}

	return &Bully{
		nodeID:      cfg.NodeID,
		priority:    cfg.Priority,
		membership:  cfg.Membership,
		logger:      cfg.Logger,
		apiKey:      cfg.APIKey,
		compression: cfg.Compression,
		interval:    cfg.Interval,
		timeout:     cfg.LeaderTimeout,
		dialTimeout: 2 * time.Second,
		maxProbes:   3,
		tracer:      otel.Tracer("election"),
	}
}

// NodeID returns this node's identifier.
func (b *Bully) NodeID() string {
	return b.nodeID
}

// Priority returns this node's election priority.
func (b *Bully) Priority() int64 {
	return b.priority
}

// LeaderID returns the current coordinator ("" when none is known).
func (b *Bully) LeaderID() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.leaderID
}

// IsLeader reports whether this node is the current coordinator.
func (b *Bully) IsLeader() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.leaderID == b.nodeID
}

// LeaderTerm returns the current election term.
func (b *Bully) LeaderTerm() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.leaderTerm
}

// Run periodically checks the leadership and triggers elections. The first
// election fires immediately on startup so a fresh cluster converges fast.
func (b *Bully) Run(ctx context.Context) {
	b.logger.Info("Starting leader election (Bully)",
		zap.String("node_id", b.nodeID),
		zap.Int64("priority", b.priority),
		zap.Duration("interval", b.interval),
	)

	b.StartElection(ctx)

	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.mu.Lock()
			isLeader := b.leaderID == b.nodeID
			leaderKnown := b.leaderID != ""
			leaderStale := leaderKnown && time.Since(b.lastLeaderSeen) > b.timeout
			b.mu.Unlock()

			// A leader must keep announcing itself, or followers would see
			// its "last seen" go stale and start a pointless election.
			if isLeader {
				b.announceLeader(ctx)
				continue
			}

			if !leaderKnown || leaderStale {
				b.logger.Debug("No healthy leader, starting election",
					zap.Bool("leader_known", leaderKnown),
					zap.Bool("leader_stale", leaderStale))
				b.StartElection(ctx)
			}
		}
	}
}

// announceLeader re-broadcasts the current leadership to every live peer,
// refreshing the followers' leader timeout.
func (b *Bully) announceLeader(ctx context.Context) {
	b.mu.Lock()
	term := b.leaderTerm
	b.mu.Unlock()

	for _, m := range b.membership.GetAlivePeers() {
		if m.ID == b.nodeID {
			continue
		}
		b.sendCoordinator(ctx, m, term)
	}
}

// StartElection runs one Bully election round: probe every live peer with a
// higher priority; if at least one answers, await a Coordinator broadcast;
// otherwise claim leadership.
func (b *Bully) StartElection(ctx context.Context) {
	b.mu.Lock()
	if b.electionRunning {
		b.mu.Unlock()
		return
	}
	b.electionRunning = true
	b.mu.Unlock()

	// Best effort: multiple goroutines may race through HandleElection.
	// The flag is cleared on every path so a lost election can retry.
	defer func() {
		b.mu.Lock()
		b.electionRunning = false
		b.mu.Unlock()
	}()

	ctx, span := b.tracer.Start(ctx, "election.start")
	defer span.End()

	higher := b.higherPriorityLivePeers()

	ack := false
	for _, peer := range higher {
		if b.sendElection(ctx, peer) {
			ack = true
		}
	}

	if !ack {
		b.BecomeLeader(ctx)
		return
	}

	// At least one higher-priority peer answered; wait for its Coordinator
	// announcement. If none arrives, the Run loop will retry next tick.
	deadline := time.Now().Add(b.timeout)
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return
		}
		b.mu.Lock()
		leader := b.leaderID
		b.mu.Unlock()
		if leader != "" {
			span.SetAttributes(attribute.Bool("election.won", false))
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// higherPriorityLivePeers returns the live members whose priority exceeds
// this node's, excluding itself.
func (b *Bully) higherPriorityLivePeers() []*cluster.Member {
	var peers []*cluster.Member
	for _, m := range b.membership.GetAlivePeers() {
		if m.ID == b.nodeID {
			continue
		}
		if m.Priority > b.priority {
			peers = append(peers, m)
		}
	}
	return peers
}

// BecomeLeader declares this node the coordinator and broadcasts the result.
func (b *Bully) BecomeLeader(ctx context.Context) {
	b.mu.Lock()
	b.leaderID = b.nodeID
	b.leaderPriority = b.priority
	b.leaderTerm++
	b.lastLeaderSeen = time.Now()
	term := b.leaderTerm
	b.mu.Unlock()

	b.logger.Info("Elected leader",
		zap.String("node_id", b.nodeID),
		zap.Int64("priority", b.priority),
		zap.Int64("term", term),
	)

	ctx, span := b.tracer.Start(ctx, "election.become_leader")
	defer span.End()

	for _, m := range b.membership.GetAlivePeers() {
		if m.ID == b.nodeID {
			continue
		}
		b.sendCoordinator(ctx, m, term)
	}
}

// HandleElection answers an election probe. If the candidate has a lower
// priority, this node answers OK and starts its own election, since it is a
// more suitable leader.
func (b *Bully) HandleElection(ctx context.Context, req *pb.ElectionRequest) *pb.ElectionResponse {
	resp := &pb.ElectionResponse{
		NodeId:   b.nodeID,
		Ok:       true,
		Priority: b.priority,
	}

	if req.Priority < b.priority {
		go b.StartElection(ctx)
	}

	return resp
}

// HandleCoordinator records the announced leader, stamping the last-seen
// time so the Run loop does not trigger another election for a healthy
// leader.
//
// Priority dominates: a coordinator is only accepted when it is at least as
// eligible as the current one, or when the current leader has gone stale and
// the announcement carries a newer term. This guarantees a lower-priority
// node can never displace a healthy higher-priority leader (e.g. after a
// transient probe failure), while still allowing failover once the leader is
// actually gone.
func (b *Bully) HandleCoordinator(ctx context.Context, req *pb.CoordinatorRequest) *pb.CoordinatorResponse {
	b.mu.Lock()

	fresh := b.leaderID != "" && time.Since(b.lastLeaderSeen) <= b.timeout
	accept := req.Priority > b.leaderPriority ||
		(req.Priority == b.leaderPriority && req.Term >= b.leaderTerm) ||
		(!fresh && req.Term > b.leaderTerm)

	if accept {
		b.leaderID = req.NodeId
		b.leaderPriority = req.Priority
		b.leaderTerm = req.Term
		b.lastLeaderSeen = time.Now()
	}
	b.mu.Unlock()

	b.logger.Info("Leader announced",
		zap.String("leader", req.NodeId),
		zap.Int64("priority", req.Priority),
		zap.Int64("term", req.Term),
		zap.Bool("accepted", accept),
	)

	return &pb.CoordinatorResponse{Success: true}
}

// sendElection probes a peer with an Election RPC, returning true on OK. The
// probe uses a blocking dial so a live peer is never falsely reported down
// due to a race between connection establishment and the first RPC, and is
// retried a few times so a transient failure does not make a lower-priority
// node wrongly believe it is the highest priority peer.
func (b *Bully) sendElection(ctx context.Context, peer *cluster.Member) bool {
	for attempt := 0; attempt < b.maxProbes; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, b.dialTimeout)

		opts := append(grpcutil.DialOptions(b.apiKey, b.compression), grpc.WithBlock())
		conn, err := grpc.DialContext(callCtx, peer.Address, opts...)
		if err == nil {
			client := pb.NewCounterServiceClient(conn)
			resp, err := client.Election(callCtx, &pb.ElectionRequest{
				NodeId:   b.nodeID,
				Priority: b.priority,
			})
			_ = conn.Close()
			if err == nil {
				cancel()
				return resp.Ok
			}
		}

		cancel()
		b.logger.Debug("Election probe failed",
			zap.String("peer", peer.ID),
			zap.Int("attempt", attempt+1),
			zap.Error(err))

		select {
		case <-ctx.Done():
			return false
		case <-time.After(150 * time.Millisecond):
		}
	}
	return false
}

// sendCoordinator broadcasts the leadership announcement to a peer.
func (b *Bully) sendCoordinator(ctx context.Context, peer *cluster.Member, term int64) {
	callCtx, cancel := context.WithTimeout(ctx, b.dialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(callCtx, peer.Address, grpcutil.DialOptions(b.apiKey, b.compression)...)
	if err != nil {
		return
	}
	defer conn.Close()

	client := pb.NewCounterServiceClient(conn)
	if _, err := client.Coordinator(callCtx, &pb.CoordinatorRequest{
		NodeId:   b.nodeID,
		Priority: b.priority,
		Term:     term,
	}); err != nil {
		b.logger.Debug("Coordinator broadcast failed",
			zap.String("peer", peer.ID), zap.Error(err))
	}
}
