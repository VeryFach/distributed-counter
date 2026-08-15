// Package partition verifies that the cluster reaches eventual consistency
// across a network partition: while a node is cut off, the remaining nodes
// keep making progress; once the partition heals, the isolated node catches
// up to the exact same value. This is the "Network Partition Test" from
// info.md (Phase 7, feature 22).
package partition

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VeryFach/distributed-counter/test/harness"
)

const timeout = 15 * time.Second

func TestPartitionThenHeal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c := harness.Start(t, 3, harness.Options{
		GossipInterval:  200 * time.Millisecond,
		CircuitCooldown: 500 * time.Millisecond,
	})

	// Reset to a deterministic baseline and wait for the cluster to settle.
	for i := range c.Nodes {
		c.Reset(ctx, t, i)
	}
	c.WaitConverged(t, 0, timeout)

	// ============================================================
	// Partition: cut node 2 off from the rest of the cluster.
	// ============================================================
	t.Log("partitioning node-2 ...")
	c.StopNode(2)

	// While partitioned, nodes 0 and 1 keep making progress.
	c.Increment(ctx, t, 0, 10)

	// Nodes 0 and 1 must converge to 10 without node 2.
	waitRunningConverged(t, c, 10, timeout)

	// The isolated node must be behind: it never saw the +10.
	if got := c.Nodes[2].Value(); got != 0 {
		t.Fatalf("partitioned node-2 unexpectedly ahead: value=%d want=0", got)
	}
	t.Logf("partitioned node-2 is behind (value=%d) as expected", c.Nodes[2].Value())

	// ============================================================
	// Heal: bring node 2 back, it must catch up on its own.
	// ============================================================
	t.Log("healing partition (restarting node-2) ...")
	c.StartNode(2)

	c.WaitConverged(t, 10, timeout)
}

func TestConcurrentUpdatesAcrossPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c := harness.Start(t, 3, harness.Options{
		GossipInterval:  200 * time.Millisecond,
		CircuitCooldown: 500 * time.Millisecond,
	})

	for i := range c.Nodes {
		c.Reset(ctx, t, i)
	}
	c.WaitConverged(t, 0, timeout)

	// Partition node 2.
	c.StopNode(2)

	// Both live nodes mutate concurrently while node 2 is isolated.
	c.Increment(ctx, t, 0, 5)
	c.Increment(ctx, t, 1, 7)

	waitRunningConverged(t, c, 12, timeout)

	if got := c.Nodes[2].Value(); got != 0 {
		t.Fatalf("partitioned node-2 unexpectedly ahead: value=%d want=0", got)
	}

	// Heal and expect full convergence to 5 + 7 = 12.
	c.StartNode(2)
	c.WaitConverged(t, 12, timeout)
}

func TestSequentialPartitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	c := harness.Start(t, 3, harness.Options{
		GossipInterval:  200 * time.Millisecond,
		CircuitCooldown: 500 * time.Millisecond,
	})

	for i := range c.Nodes {
		c.Reset(ctx, t, i)
	}
	c.WaitConverged(t, 0, timeout)

	expected := int64(0)
	for round := 1; round <= 3; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			// Isolate a different node each round.
			isolated := (round) % 3

			c.StopNode(isolated)

			// All non-isolated nodes increment.
			for i := range c.Nodes {
				if i != isolated {
					c.Increment(ctx, t, i, 2)
				}
			}
			expected += int64((2 * (len(c.Nodes) - 1)))

			waitRunningConverged(t, c, expected, timeout)

			if got := c.Nodes[isolated].Value(); got == expected {
				t.Fatalf("isolated node-2 should lag, value=%d expected-before-heal=%d", got, expected)
			}

			c.StartNode(isolated)
			c.WaitConverged(t, expected, timeout)
		})
	}
}

// waitRunningConverged waits until every RUNNING node reaches expected,
// ignoring stopped (partitioned) nodes.
func waitRunningConverged(t *testing.T, c *harness.Cluster, expected int64, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		allEqual := true
		for _, node := range c.Nodes {
			if !node.Running() {
				continue
			}
			if node.Value() != expected {
				allEqual = false
				break
			}
		}

		if allEqual {
			t.Logf("running nodes converged to %d", expected)
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("running nodes did not converge: expected=%d running_values=%v", expected, runningValues(c))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func runningValues(c *harness.Cluster) []int64 {
	values := make([]int64, 0, len(c.Nodes))
	for _, node := range c.Nodes {
		if node.Running() {
			values = append(values, node.Value())
		}
	}
	return values
}
