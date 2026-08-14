// Package harness starts an in-process distributed-counter cluster for
// tests and benchmarks. Every node runs a real gRPC server plus a gossip
// engine on 127.0.0.1, so tests can simulate network partitions by stopping
// and restarting individual nodes without needing Docker.
package harness

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/gossip"
	"github.com/VeryFach/distributed-counter/internal/service"
)

// Options tunes the in-process cluster.
type Options struct {
	// GossipInterval is the per-node gossip round interval. Default 200ms.
	GossipInterval time.Duration
	// CircuitFailures/Cooldown tune the per-peer circuit breaker so a
	// rejoining node is retried quickly. Defaults: 3 failures / 1s cooldown.
	CircuitFailures int
	CircuitCooldown time.Duration
}

// Node is a single in-process cluster member.
type Node struct {
	ID         string
	Addr       string
	Service    *service.CounterService
	Membership *cluster.Membership
	Gossip     *gossip.GossipEngine

	mu      sync.Mutex
	running bool
	server  *grpc.Server
	lis     net.Listener
}

// Running reports whether the node's gRPC server + gossip engine are up.
func (n *Node) Running() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.running
}

// Value returns the node's local counter value.
func (n *Node) Value() int64 {
	return n.Service.Counter().Value()
}

// Cluster is a set of wired-up in-process nodes.
type Cluster struct {
	Nodes []*Node
	opts  Options
	log   *zap.Logger
}

// Start builds a cluster of n nodes, fully connected (each node knows every
// other node) with gossip running. The cluster is torn down automatically at
// test cleanup.
func Start(t testing.TB, n int, opts Options) *Cluster {
	t.Helper()

	if opts.GossipInterval <= 0 {
		opts.GossipInterval = 200 * time.Millisecond
	}
	if opts.CircuitFailures <= 0 {
		opts.CircuitFailures = 3
	}
	if opts.CircuitCooldown <= 0 {
		opts.CircuitCooldown = time.Second
	}

	log := zap.NewNop()
	cluster := &Cluster{
		opts: opts,
		log:  log,
	}

	for i := 0; i < n; i++ {
		node, err := startNode(fmt.Sprintf("node-%d", i), log, opts)
		if err != nil {
			cluster.Close()
			t.Fatalf("harness: failed to start node %d: %v", i, err)
		}
		cluster.Nodes = append(cluster.Nodes, node)
	}

	// Wire up full membership: every node knows every other node.
	for _, a := range cluster.Nodes {
		for _, b := range cluster.Nodes {
			if a != b {
				a.Membership.AddMember(b.ID, b.Addr)
			}
		}
	}

	for _, node := range cluster.Nodes {
		go node.Gossip.Start()
	}

	t.Cleanup(cluster.Close)
	return cluster
}

func startNode(id string, log *zap.Logger, opts Options) (*Node, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	svc := service.NewCounterService(id, 0, log)
	mem := cluster.NewMembership(id)
	mem.AddMember(id, lis.Addr().String())
	svc.SetCluster(mem)

	eng := gossip.NewGossipEngine(id, svc.Counter(), svc.Clock(), mem, opts.GossipInterval, log)
	eng.SetCircuitBreakerConfig(opts.CircuitFailures, opts.CircuitCooldown)

	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(10*1024*1024),
		grpc.MaxSendMsgSize(10*1024*1024),
	)
	pb.RegisterCounterServiceServer(srv, svc)

	go func() {
		_ = srv.Serve(lis)
	}()

	return &Node{
		ID:         id,
		Addr:       lis.Addr().String(),
		Service:    svc,
		Membership: mem,
		Gossip:     eng,
		server:     srv,
		lis:        lis,
		running:    true,
	}, nil
}

// StopNode simulates a network partition by halting the node's gRPC server
// and gossip engine. The membership records are kept, so the rest of the
// cluster still believes the node is alive.
func (c *Cluster) StopNode(i int) {
	node := c.Nodes[i]

	node.mu.Lock()
	if !node.running {
		node.mu.Unlock()
		return
	}
	node.Gossip.Stop()
	node.server.GracefulStop()
	node.running = false
	node.mu.Unlock()
}

// StartNode restarts a node stopped with StopNode, re-binding the same
// address and restarting gossip. Simulates the partition healing.
func (c *Cluster) StartNode(i int) {
	node := c.Nodes[i]

	node.mu.Lock()
	defer node.mu.Unlock()
	if node.running {
		return
	}

	lis, err := rebindListen(node.Addr, 50)
	if err != nil {
		panic(fmt.Sprintf("harness: failed to re-listen on %s: %v", node.Addr, err))
	}

	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(10*1024*1024),
		grpc.MaxSendMsgSize(10*1024*1024),
	)
	pb.RegisterCounterServiceServer(srv, node.Service)

	eng := gossip.NewGossipEngine(node.ID, node.Service.Counter(), node.Service.Clock(), node.Membership, c.opts.GossipInterval, c.log)
	eng.SetCircuitBreakerConfig(c.opts.CircuitFailures, c.opts.CircuitCooldown)

	node.server = srv
	node.lis = lis
	node.Gossip = eng
	node.running = true

	go func() {
		_ = srv.Serve(lis)
	}()
	go eng.Start()
}

// Client returns a gRPC client connected to node i.
func (c *Cluster) Client(ctx context.Context, i int) (pb.CounterServiceClient, func(), error) {
	conn, err := grpc.DialContext(
		ctx,
		c.Nodes[i].Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, err
	}
	return pb.NewCounterServiceClient(conn), func() { _ = conn.Close() }, nil
}

// ClientValue reads the counter value of node i through gRPC.
func (c *Cluster) ClientValue(ctx context.Context, t testing.TB, i int) int64 {
	t.Helper()
	client, closeConn, err := c.Client(ctx, i)
	if err != nil {
		t.Fatalf("harness: dial node %d: %v", i, err)
	}
	defer closeConn()

	resp, err := client.GetValue(ctx, &pb.GetValueRequest{})
	if err != nil {
		t.Fatalf("harness: GetValue node %d: %v", i, err)
	}
	return resp.CurrentValue
}

// Increment adds delta to node i through gRPC.
func (c *Cluster) Increment(ctx context.Context, t testing.TB, i int, delta int32) {
	t.Helper()
	client, closeConn, err := c.Client(ctx, i)
	if err != nil {
		t.Fatalf("harness: dial node %d: %v", i, err)
	}
	defer closeConn()

	if _, err := client.Increment(ctx, &pb.IncrementRequest{Delta: delta}); err != nil {
		t.Fatalf("harness: Increment node %d: %v", i, err)
	}
}

// Reset resets the counter on node i through gRPC.
func (c *Cluster) Reset(ctx context.Context, t testing.TB, i int) {
	t.Helper()
	client, closeConn, err := c.Client(ctx, i)
	if err != nil {
		t.Fatalf("harness: dial node %d: %v", i, err)
	}
	defer closeConn()

	if _, err := client.Reset(ctx, &pb.ResetRequest{}); err != nil {
		t.Fatalf("harness: Reset node %d: %v", i, err)
	}
}

// Values returns the local counter value of every node, including stopped
// ones (whose value is read from the in-memory CRDT).
func (c *Cluster) Values() []int64 {
	values := make([]int64, len(c.Nodes))
	for i, node := range c.Nodes {
		values[i] = node.Value()
	}
	return values
}

// WaitConverged polls the local value of every node until all of them equal
// expected, or fails after timeout. A stopped node's value is included and
// can therefore be "behind" on purpose.
func (c *Cluster) WaitConverged(t testing.TB, expected int64, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		values := c.Values()
		allEqual := true
		for _, v := range values {
			if v != expected {
				allEqual = false
				break
			}
		}
		if allEqual {
			t.Logf("cluster converged to %d", expected)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("cluster did not converge: expected=%d values=%v", expected, values)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Close stops every node's server and gossip engine.
func (c *Cluster) Close() {
	for _, node := range c.Nodes {
		node.mu.Lock()
		if node.running {
			node.Gossip.Stop()
			node.server.GracefulStop()
			node.running = false
		}
		node.mu.Unlock()
	}
}

// rebindListen re-listens on addr, retrying because the OS may briefly hold
// the port after GracefulStop before it is fully released.
func rebindListen(addr string, attempts int) (net.Listener, error) {
	var err error
	for i := 0; i < attempts; i++ {
		var lis net.Listener
		lis, err = net.Listen("tcp", addr)
		if err == nil {
			return lis, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil, err
}
