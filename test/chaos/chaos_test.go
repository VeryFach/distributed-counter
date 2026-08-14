// Package chaos stress-tests the cluster under random node failures. The
// default (in-process) mode randomly stops/restarts nodes while a load
// generator keeps mutating the counter, then verifies two invariants:
//
//  1. Monotonicity: because only increments are issued, no node's value may
//     ever decrease, even while nodes are being partitioned and restarted.
//  2. Eventual consistency: once every node is back up, all nodes converge
//     to exactly the total number of successful increments.
//
// This is the "Chaos Testing" feature from info.md (Phase 7, feature 23).
// A Docker-based variant targeting a compose cluster is available via the
// `chaosdocker` build tag (see chaos_docker_test.go).
package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	counter "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/test/harness"
)

const chaosDuration = 5 * time.Second

func TestChaosInProcess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const numNodes = 5
	c := harness.Start(t, numNodes, harness.Options{
		GossipInterval:  100 * time.Millisecond,
		CircuitFailures: 2,
		CircuitCooldown: 300 * time.Millisecond,
	})

	for i := range c.Nodes {
		c.Reset(ctx, t, i)
	}
	c.WaitConverged(t, 0, 10*time.Second)

	// One persistent connection per node. Reusing them (instead of dialing
	// per request) keeps goroutine usage bounded during heavy load.
	conns := make([]*grpc.ClientConn, numNodes)
	clients := make([]counter.CounterServiceClient, numNodes)
	for i := range c.Nodes {
		conn, err := grpc.DialContext(
			ctx,
			c.Nodes[i].Addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("chaos: dial node-%d: %v", i, err)
		}
		conns[i] = conn
		clients[i] = counter.NewCounterServiceClient(conn)
	}
	t.Cleanup(func() {
		for _, conn := range conns {
			if conn != nil {
				_ = conn.Close()
			}
		}
	})

	var (
		loadWG  sync.WaitGroup
		chaosWG sync.WaitGroup
		stop    atomic.Bool
		success atomic.Int64
		mu      sync.Mutex
		lastVal = make([]int64, numNodes)
		errCh   = make(chan error, 64)
	)

	// Load generator: increment a random running node, tracking monotonicity.
	loadWG.Add(1)
	go func() {
		defer loadWG.Done()
		for !stop.Load() {
			i := rand.Intn(numNodes)
			if !c.Nodes[i].Running() {
				time.Sleep(5 * time.Millisecond)
				continue
			}

			// A per-call timeout keeps the generator responsive while a node
			// is stopped/restarting (the shared ctx lives for the whole test).
			callCtx, callCancel := context.WithTimeout(ctx, time.Second)
			resp, err := clients[i].Increment(callCtx, &counter.IncrementRequest{Delta: 1})
			callCancel()
			if err != nil {
				// Failures during chaos (node stopped mid-call) are expected.
				if code := status.Code(err); code != codes.Unavailable &&
					code != codes.DeadlineExceeded && code != codes.Canceled {
					errCh <- fmt.Errorf("unexpected increment error on node-%d: %w", i, err)
				}
				time.Sleep(5 * time.Millisecond)
				continue
			}

			success.Add(1)
			mu.Lock()
			if resp.CurrentValue < lastVal[i] {
				mu.Unlock()
				errCh <- fmt.Errorf("monotonicity violated: node-%d went from %d to %d", i, lastVal[i], resp.CurrentValue)
				continue
			}
			lastVal[i] = resp.CurrentValue
			mu.Unlock()

			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Chaos driver: randomly stop/restart nodes (partitions + restarts).
	chaosWG.Add(1)
	go func() {
		defer chaosWG.Done()
		deadline := time.Now().Add(chaosDuration)
		for time.Now().Before(deadline) {
			action := rand.Intn(3)
			switch action {
			case 0: // partition a random running node
				candidates := runningIndexes(c)
				if len(candidates) == 0 {
					continue
				}
				i := candidates[rand.Intn(len(candidates))]
				t.Logf("chaos: partition node-%d", i)
				c.StopNode(i)

			case 1: // heal a random stopped node
				candidates := stoppedIndexes(c)
				if len(candidates) == 0 {
					continue
				}
				i := candidates[rand.Intn(len(candidates))]
				t.Logf("chaos: heal node-%d", i)
				c.StartNode(i)

			case 2: // quick restart of a running node
				candidates := runningIndexes(c)
				if len(candidates) == 0 {
					continue
				}
				i := candidates[rand.Intn(len(candidates))]
				t.Logf("chaos: restart node-%d", i)
				c.StopNode(i)
				c.StartNode(i)
			}
			time.Sleep(150 * time.Millisecond)
		}
	}()

	// Wait for the chaos driver to finish, then stop the load generator and
	// bring every node back up for the final convergence check.
	chaosWG.Wait()
	stop.Store(true)
	loadWG.Wait()

	for i, node := range c.Nodes {
		if !node.Running() {
			t.Logf("chaos: final heal node-%d", i)
			c.StartNode(i)
		}
	}

	expected := success.Load()
	t.Logf("issued %d successful increments", expected)

	// Surface any errors collected from the load generator goroutine.
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	c.WaitConverged(t, expected, 20*time.Second)
}

func runningIndexes(c *harness.Cluster) []int {
	idx := make([]int, 0, len(c.Nodes))
	for i, node := range c.Nodes {
		if node.Running() {
			idx = append(idx, i)
		}
	}
	return idx
}

func stoppedIndexes(c *harness.Cluster) []int {
	idx := make([]int, 0, len(c.Nodes))
	for i, node := range c.Nodes {
		if !node.Running() {
			idx = append(idx, i)
		}
	}
	return idx
}
