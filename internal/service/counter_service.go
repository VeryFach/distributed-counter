package service

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/crdt"
	"github.com/VeryFach/distributed-counter/internal/metrics"
	"github.com/VeryFach/distributed-counter/internal/persistence"
	"github.com/VeryFach/distributed-counter/pkg/grpcutil"
)

// defaultCounter is the counter used by clients that do not specify a name,
// preserving the single-counter API for existing callers.
const defaultCounter = "default"

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
	wal      *persistence.WALStore
	onReset  func()

	// lastSyncClock tracks, per sender, the vector clock we last replied
	// with, so delta replies never resend identical state.
	mu            sync.Mutex
	lastSyncClock map[string]map[string]int64

	// tags holds the tags registered per counter via CreateCounter.
	tags map[string][]string

	// counterShards is the number of shards used by Shard() for the
	// deterministic assignment of counters to shards (default 1).
	counterShards int

	// apiKey enables auth on the SWIM indirect probe dials.
	apiKey      string
	compression bool

	// tracer emits business-level spans for counter operations.
	tracer trace.Tracer
}

func NewCounterService(nodeID string, port int, logger *zap.Logger) *CounterService {
	return &CounterService{
		nodeID:        nodeID,
		port:          port,
		counter:       crdt.NewPNCounter(nodeID),
		clock:         crdt.NewVectorClock(nodeID),
		logger:        logger,
		lastSyncClock: make(map[string]map[string]int64),
		tags:          make(map[string][]string),
		counterShards: 1,
		tracer:        otel.Tracer("counter-service"),
	}
}

func (s *CounterService) getPort() int {
	return s.port
}

// normalizeName maps the empty counter name to the default counter.
func normalizeName(name string) string {
	if name == "" {
		return defaultCounter
	}
	return name
}

func (s *CounterService) Increment(ctx context.Context, req *pb.IncrementRequest) (*pb.CounterResponse, error) {
	name := normalizeName(req.CounterName)
	s.logger.Debug("Increment called", zap.Int32("delta", req.Delta), zap.String("counter", name))

	delta := int64(req.Delta)
	if delta == 0 {
		delta = 1
	}

	_, span := s.tracer.Start(ctx, "counter.increment",
		trace.WithAttributes(
			attribute.String("node.id", s.nodeID),
			attribute.String("counter.name", name),
			attribute.Int64("delta", delta),
		),
	)
	defer span.End()

	s.counter.IncrementName(name, delta)
	s.clock.IncrementName(name)

	metrics.IncIncrementTotal(s.nodeID)
	metrics.UpdateCounterValue(s.nodeID, s.counter.ValueName(name))

	s.walAppend("increment", name, delta, nil, nil, nil)
	s.persist()

	span.SetAttributes(attribute.Int64("value", s.counter.ValueName(name)))

	return s.buildResponse(name), nil
}

func (s *CounterService) Decrement(ctx context.Context, req *pb.DecrementRequest) (*pb.CounterResponse, error) {
	name := normalizeName(req.CounterName)
	s.logger.Debug("Decrement called", zap.Int32("delta", req.Delta), zap.String("counter", name))

	delta := int64(req.Delta)
	if delta == 0 {
		delta = 1
	}

	_, span := s.tracer.Start(ctx, "counter.decrement",
		trace.WithAttributes(
			attribute.String("node.id", s.nodeID),
			attribute.String("counter.name", name),
			attribute.Int64("delta", delta),
		),
	)
	defer span.End()

	s.counter.DecrementName(name, delta)
	s.clock.IncrementName(name)

	metrics.IncDecrementTotal(s.nodeID)
	metrics.UpdateCounterValue(s.nodeID, s.counter.ValueName(name))

	s.walAppend("decrement", name, delta, nil, nil, nil)
	s.persist()

	span.SetAttributes(attribute.Int64("value", s.counter.ValueName(name)))

	return s.buildResponse(name), nil
}

func (s *CounterService) Reset(ctx context.Context, req *pb.ResetRequest) (*pb.CounterResponse, error) {
	name := normalizeName(req.CounterName)
	s.logger.Info("Reset called", zap.String("node_id", s.nodeID), zap.String("counter", name))

	prev := s.counter.ValueName(name)
	_, span := s.tracer.Start(ctx, "counter.reset",
		trace.WithAttributes(
			attribute.String("node.id", s.nodeID),
			attribute.String("counter.name", name),
			attribute.Int64("previous_value", prev),
		),
	)
	defer span.End()

	s.counter.ResetName(name)
	s.clock.ResetName(name)

	// Clear per-sender sync baselines: the vector clock for this counter is
	// now zero, so any retained baseline would make delta gossip skip
	// everything and leave this node unable to exchange state until a slow
	// reconciliation.
	s.mu.Lock()
	s.lastSyncClock = make(map[string]map[string]int64)
	s.mu.Unlock()

	if s.onReset != nil {
		s.onReset()
	}

	metrics.UpdateCounterValue(s.nodeID, s.counter.ValueName(name))

	s.walAppend("reset", name, 0, nil, nil, nil)
	s.persist()

	return s.buildResponse(name), nil
}

func (s *CounterService) GetValue(ctx context.Context, req *pb.GetValueRequest) (*pb.CounterResponse, error) {
	return s.buildResponse(normalizeName(req.CounterName)), nil
}

// CreateCounter registers a counter's tags (counters are created lazily on
// first increment; this RPC only attaches metadata) and returns its info.
func (s *CounterService) CreateCounter(ctx context.Context, req *pb.CreateCounterRequest) (*pb.CounterInfo, error) {
	name := normalizeName(req.Name)

	if len(req.Tags) > 0 {
		s.mu.Lock()
		s.tags[name] = append([]string(nil), req.Tags...)
		s.mu.Unlock()
	}

	s.logger.Info("CreateCounter",
		zap.String("node_id", s.nodeID),
		zap.String("counter", name),
		zap.Strings("tags", req.Tags),
	)
	return s.buildCounterInfo(name), nil
}

// ListCounters enumerates every known counter, optionally filtered to those
// carrying a given tag. The default counter is always included when nothing
// else exists, so single-counter clients see a usable list.
func (s *CounterService) ListCounters(ctx context.Context, req *pb.ListCountersRequest) (*pb.ListCountersResponse, error) {
	names := make(map[string]bool)
	for _, n := range s.counter.Names() {
		names[n] = true
	}

	s.mu.Lock()
	for n := range s.tags {
		names[n] = true
	}
	tags := make(map[string][]string, len(s.tags))
	for k, v := range s.tags {
		tags[k] = append([]string(nil), v...)
	}
	s.mu.Unlock()

	if len(names) == 0 {
		names[defaultCounter] = true
	}

	res := make([]*pb.CounterInfo, 0, len(names))
	for n := range names {
		if req.Tag != "" && !containsTag(tags[n], req.Tag) {
			continue
		}
		res = append(res, s.buildCounterInfo(n))
	}

	sort.Slice(res, func(i, j int) bool { return res[i].Name < res[j].Name })
	return &pb.ListCountersResponse{Counters: res}, nil
}

func containsTag(tags []string, tag string) bool {
	for _, t := range tags {
		if t == tag {
			return true
		}
	}
	return false
}

// buildCounterInfo assembles a CounterInfo for the given counter, including
// its deterministic shard assignment.
func (s *CounterService) buildCounterInfo(name string) *pb.CounterInfo {
	s.mu.Lock()
	tags := append([]string(nil), s.tags[name]...)
	s.mu.Unlock()

	return &pb.CounterInfo{
		Name:         name,
		CurrentValue: s.counter.ValueName(name),
		Shard:        s.Shard(name),
		Tags:         tags,
	}
}

// Shard assigns a counter to a shard deterministically via FNV-1a, so every
// node agrees on where a counter lives. With one shard every counter maps to
// shard 0 (no partitioning).
func (s *CounterService) Shard(name string) uint32 {
	s.mu.Lock()
	n := s.counterShards
	s.mu.Unlock()

	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return h.Sum32() % uint32(n)
}

// SetShardCount configures the number of logical shards for Shard().
func (s *CounterService) SetShardCount(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n < 1 {
		n = 1
	}
	s.counterShards = n
}

func (s *CounterService) GetNodeInfo(ctx context.Context, req *pb.GetNodeInfoRequest) (*pb.NodeInfo, error) {
	return &pb.NodeInfo{
		NodeId:       s.nodeID,
		Address:      fmt.Sprintf("localhost:%d", s.getPort()),
		CounterValue: s.counter.ValueName(defaultCounter),
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
				Status:        member.Status.ToProto(),
			})
		}
	}

	if len(members) == 0 && req.NodeId != "" {
		members = append(members, &pb.Member{
			NodeId:        req.NodeId,
			Address:       req.Address,
			IsActive:      true,
			LastHeartbeat: time.Now().Unix(),
			Status:        pb.MemberStatus_MEMBER_ALIVE,
		})
	}

	return stream.Send(&pb.MemberList{
		Members: members,
		Version: time.Now().Unix(),
	})
}

func (s *CounterService) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if s.cluster != nil {
		// Prefer the sender's advertised address so members are never
		// recorded with an ephemeral source port.
		remoteAddress := req.Address
		if remoteAddress == "" {
			if remotePeer, ok := peer.FromContext(ctx); ok && remotePeer.Addr != nil {
				remoteAddress = remotePeer.Addr.String()
			}
		}
		s.cluster.AddOrUpdateMember(req.NodeId, remoteAddress, true, time.Unix(req.Timestamp, 0))
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
					Status:        member.Status.ToProto(),
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

// SwimPing is the SWIM direct probe. Reaching this handler means the node
// is alive, so it always acknowledges.
func (s *CounterService) SwimPing(ctx context.Context, req *pb.SwimPingRequest) (*pb.SwimPingResponse, error) {
	return &pb.SwimPingResponse{
		NodeId:    s.nodeID,
		Alive:     true,
		MessageId: req.MessageId,
	}, nil
}

// SwimPingReq is the SWIM indirect probe. The requester could not reach the
// target directly, so this node pings the target on its behalf and reports
// the result back.
func (s *CounterService) SwimPingReq(ctx context.Context, req *pb.SwimPingReqRequest) (*pb.SwimPingReqResponse, error) {
	alive := false
	if s.cluster != nil {
		if target, exists := s.cluster.GetMember(req.TargetNodeId); exists && target.Status == cluster.StatusAlive {
			alive = s.pingTarget(ctx, target.Address)
		}
	}

	return &pb.SwimPingReqResponse{
		NodeId:    s.nodeID,
		Alive:     alive,
		MessageId: req.MessageId,
	}, nil
}

// pingTarget performs a direct SWIM ping to the given address.
func (s *CounterService) pingTarget(ctx context.Context, address string) bool {
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	opts := append(grpcutil.DialOptions(s.apiKey, s.compression), grpc.WithBlock())
	conn, err := grpc.DialContext(
		pingCtx,
		address,
		opts...,
	)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := pb.NewCounterServiceClient(conn)
	resp, err := client.SwimPing(pingCtx, &pb.SwimPingRequest{
		FromNodeId:   s.nodeID,
		TargetNodeId: address,
	})
	if err != nil {
		return false
	}
	return resp.Alive
}

func (s *CounterService) buildResponse(name string) *pb.CounterResponse {
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
		CounterName:  name,
		CurrentValue: s.counter.ValueName(name),
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

		// Merge membership lifecycle gossip (Suspect/Dead/Left).
		if s.cluster != nil && len(update.Membership) > 0 {
			s.cluster.ApplyMembership(update.Membership)
		}

		metrics.UpdateCounterValue(
			s.nodeID,
			s.counter.Value(),
		)
		metrics.IncGossipReceived(s.nodeID)

		s.walAppend("merge", "", 0, update.PositiveState, update.NegativeState, update.VectorClock)
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
		Membership:      s.membershipGossip(),
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
		Membership:      s.membershipGossip(),
	}
}

// membershipGossip returns the current member status map to piggyback on
// state updates, or nil when the cluster is not wired yet.
func (s *CounterService) membershipGossip() map[string]pb.MemberStatus {
	if s.cluster == nil {
		return nil
	}
	return s.cluster.GossipMembership()
}

// persist writes the current CRDT state to the configured store. When a WAL
// is enabled the log is the durability mechanism and the store is refreshed
// only by the periodic snapshot loop; without a WAL the store is updated on
// every operation.
func (s *CounterService) persist() {
	if s.wal != nil {
		return
	}
	if s.store == nil {
		return
	}

	s.mu.Lock()
	tags := make(map[string][]string, len(s.tags))
	for k, v := range s.tags {
		tags[k] = append([]string(nil), v...)
	}
	s.mu.Unlock()

	state := persistence.CounterState{
		Positive: s.counter.Positive(),
		Negative: s.counter.Negative(),
		Clock:    s.clock.State(),
		Tags:     tags,
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

	if len(state.Tags) > 0 {
		s.mu.Lock()
		for k, v := range state.Tags {
			s.tags[k] = append([]string(nil), v...)
		}
		s.mu.Unlock()
	}

	metrics.UpdateCounterValue(s.nodeID, s.counter.ValueName(defaultCounter))

	s.logger.Info("Restored counter state from persistence",
		zap.Int64("value", s.counter.ValueName(defaultCounter)))
}

// SetStore injects the persistence store used to survive restarts.
func (s *CounterService) SetStore(store persistence.Store) {
	s.store = store
}

// SetClientConfig configures auth + compression for outbound cluster dials
// made by this service (SWIM indirect probes).
func (s *CounterService) SetClientConfig(apiKey string, compression bool) {
	s.apiKey = apiKey
	s.compression = compression
}

// SetOnReset registers a callback invoked after a local Reset, letting the
// gossip engine drop its stale delta baselines.
func (s *CounterService) SetOnReset(fn func()) {
	s.onReset = fn
}

// SetWAL injects the write-ahead log used to survive crashes between
// snapshots.
func (s *CounterService) SetWAL(wal *persistence.WALStore) {
	s.wal = wal
}

// walAppend records a mutation in the WAL before it is considered durable.
func (s *CounterService) walAppend(op, counter string, delta int64, positive, negative, clock map[string]int64) {
	if s.wal == nil {
		return
	}

	if err := s.wal.AppendCounter(s.nodeID, counter, op, delta, positive, negative, clock); err != nil {
		s.logger.Error("Failed to append WAL entry", zap.String("op", op), zap.Error(err))
	}
}

// ReplayWAL applies previously logged mutations back into the CRDT after a
// restart, restoring any state that fell between snapshots.
func (s *CounterService) ReplayWAL() error {
	if s.wal == nil {
		return nil
	}

	entries, err := s.wal.Replay(s.nodeID)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		switch entry.Op {
		case "increment":
			s.counter.IncrementName(entry.Counter, entry.Delta)
			s.clock.IncrementName(entry.Counter)
		case "decrement":
			s.counter.DecrementName(entry.Counter, entry.Delta)
			s.clock.IncrementName(entry.Counter)
		case "reset":
			s.counter.ResetName(entry.Counter)
			s.clock.ResetName(entry.Counter)
		case "merge":
			remote := crdt.NewPNCounter("")
			remote.SetPositive(entry.Positive)
			remote.SetNegative(entry.Negative)
			s.counter.Merge(remote)
			s.clock.MergeMap(entry.Clock)
		}
	}

	if len(entries) > 0 {
		metrics.UpdateCounterValue(s.nodeID, s.counter.ValueName(defaultCounter))
		s.logger.Info("Replayed WAL entries",
			zap.Int("count", len(entries)),
			zap.Int64("value", s.counter.ValueName(defaultCounter)))
	}

	return nil
}

// Snapshot persists the full CRDT state to the store and truncates the WAL,
// keeping the log bounded between snapshots.
func (s *CounterService) Snapshot() {
	if s.store == nil {
		return
	}

	s.mu.Lock()
	tags := make(map[string][]string, len(s.tags))
	for k, v := range s.tags {
		tags[k] = append([]string(nil), v...)
	}
	s.mu.Unlock()

	state := persistence.CounterState{
		Positive: s.counter.Positive(),
		Negative: s.counter.Negative(),
		Clock:    s.clock.State(),
		Tags:     tags,
	}

	if err := s.store.Save(s.nodeID, state); err != nil {
		s.logger.Error("Failed to take snapshot", zap.Error(err))
		return
	}

	if s.wal != nil {
		if err := s.wal.Truncate(s.nodeID); err != nil {
			s.logger.Error("Failed to truncate WAL after snapshot", zap.Error(err))
		}
	}

	s.logger.Debug("Snapshot taken", zap.Int64("value", s.counter.ValueName(defaultCounter)))
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
