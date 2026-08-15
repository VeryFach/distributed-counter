// Package multicounter verifies the multi-counter feature: independent named
// counters, tagged counters and deterministic counter sharding. It reuses the
// in-process harness so the same CRDT code path as the real cluster is
// exercised without Docker.
package multicounter

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/test/harness"
)

const timeout = 15 * time.Second

// incr issues a named increment against node i.
func incr(ctx context.Context, t *testing.T, c *harness.Cluster, i int, name string, delta int32) {
	t.Helper()
	client, closeConn, err := c.Client(ctx, i)
	if err != nil {
		t.Fatalf("dial node %d: %v", i, err)
	}
	defer closeConn()
	if _, err := client.Increment(ctx, &pb.IncrementRequest{Delta: delta, CounterName: name}); err != nil {
		t.Fatalf("increment %q on node %d: %v", name, i, err)
	}
}

// reset issues a named reset against node i.
func reset(ctx context.Context, t *testing.T, c *harness.Cluster, i int, name string) {
	t.Helper()
	client, closeConn, err := c.Client(ctx, i)
	if err != nil {
		t.Fatalf("dial node %d: %v", i, err)
	}
	defer closeConn()
	if _, err := client.Reset(ctx, &pb.ResetRequest{CounterName: name}); err != nil {
		t.Fatalf("reset %q on node %d: %v", name, i, err)
	}
}

// getValue reads the named counter through gRPC from node i.
func getValue(ctx context.Context, t *testing.T, c *harness.Cluster, i int, name string) int64 {
	t.Helper()
	client, closeConn, err := c.Client(ctx, i)
	if err != nil {
		t.Fatalf("dial node %d: %v", i, err)
	}
	defer closeConn()
	resp, err := client.GetValue(ctx, &pb.GetValueRequest{CounterName: name})
	if err != nil {
		t.Fatalf("get %q on node %d: %v", name, i, err)
	}
	return resp.CurrentValue
}

// counterValues returns the local named-counter value of every running node.
func counterValues(c *harness.Cluster, name string) []int64 {
	values := make([]int64, 0, len(c.Nodes))
	for _, node := range c.Nodes {
		if !node.Running() {
			continue
		}
		values = append(values, node.Service.Counter().ValueName(name))
	}
	return values
}

// waitConverged waits until every running node's local value for the named
// counter equals expected.
func waitConverged(t *testing.T, c *harness.Cluster, name string, expected int64, d time.Duration) {
	t.Helper()

	deadline := time.Now().Add(d)
	for {
		allEqual := true
		for _, v := range counterValues(c, name) {
			if v != expected {
				allEqual = false
				break
			}
		}
		if allEqual {
			t.Logf("counter %q converged to %d", name, expected)
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("counter %q did not converge: expected=%d values=%v", name, expected, counterValues(c, name))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestIndependentCounters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c := harness.Start(t, 3, harness.Options{
		GossipInterval: 200 * time.Millisecond,
	})

	// Different counters on different nodes, plus the default counter.
	incr(ctx, t, c, 0, "post_1", 5)
	incr(ctx, t, c, 1, "post_2", 3)
	incr(ctx, t, c, 2, "post_3", 2)
	incr(ctx, t, c, 0, "", 7) // default counter

	// Each counter must converge to its own total, independent of the others.
	waitConverged(t, c, "post_1", 5, timeout)
	waitConverged(t, c, "post_2", 3, timeout)
	waitConverged(t, c, "post_3", 2, timeout)
	waitConverged(t, c, "default", 7, timeout)

	// And the counters must not leak into one another.
	for _, v := range counterValues(c, "post_1") {
		if v != 5 {
			t.Fatalf("post_1 leaked: %v", counterValues(c, "post_1"))
		}
	}

	// GetValue through gRPC must agree with the local CRDT.
	if got := getValue(ctx, t, c, 2, "post_2"); got != 3 {
		t.Fatalf("GetValue(post_2) on node 2 = %d, want 3", got)
	}
}

func TestCounterResetIsScoped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c := harness.Start(t, 3, harness.Options{
		GossipInterval: 200 * time.Millisecond,
	})

	incr(ctx, t, c, 0, "post_1", 10)
	incr(ctx, t, c, 0, "post_2", 4)
	waitConverged(t, c, "post_1", 10, timeout)
	waitConverged(t, c, "post_2", 4, timeout)

	// Reset only post_1 on every node (resets are per-node, like the single
	// counter design), leaving post_2 untouched.
	for i := range c.Nodes {
		reset(ctx, t, c, i, "post_1")
	}
	waitConverged(t, c, "post_1", 0, timeout)

	// post_2 must be unaffected.
	waitConverged(t, c, "post_2", 4, timeout)

	// The default counter is independent as well.
	incr(ctx, t, c, 1, "", 2)
	waitConverged(t, c, "default", 2, timeout)
	if got := getValue(ctx, t, c, 0, "post_2"); got != 4 {
		t.Fatalf("post_2 changed after resetting post_1: got %d", got)
	}
}

func TestTaggedCounters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c := harness.Start(t, 1, harness.Options{})
	client, closeConn, err := c.Client(ctx, 0)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer closeConn()

	// Create counters with tags (metadata is local to the node).
	for _, tc := range []struct {
		name string
		tags []string
	}{
		{"post_1", []string{"likes", "popular"}},
		{"post_2", []string{"likes"}},
		{"post_3", []string{"views"}},
	} {
		if _, err := client.CreateCounter(ctx, &pb.CreateCounterRequest{Name: tc.name, Tags: tc.tags}); err != nil {
			t.Fatalf("create counter %s: %v", tc.name, err)
		}
	}

	// Filter by a tag.
	resp, err := client.ListCounters(ctx, &pb.ListCountersRequest{Tag: "likes"})
	if err != nil {
		t.Fatalf("list counters: %v", err)
	}

	names := make(map[string]bool)
	for _, ci := range resp.Counters {
		names[ci.Name] = true
	}
	if !names["post_1"] || !names["post_2"] || names["post_3"] {
		t.Fatalf("tag filter mismatch: %v", names)
	}

	// Tags come back with the counter info.
	for _, ci := range resp.Counters {
		if ci.Name == "post_1" {
			ok := false
			for _, tag := range ci.Tags {
				if tag == "popular" {
					ok = true
				}
			}
			if !ok {
				t.Fatalf("post_1 tags missing: %v", ci.Tags)
			}
		}
	}
}

func TestCounterSharding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c := harness.Start(t, 3, harness.Options{})
	// Every node must agree on the same shard layout.
	for _, node := range c.Nodes {
		node.Service.SetShardCount(3)
	}

	client, closeConn, err := c.Client(ctx, 0)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer closeConn()

	// Fresh counters get a deterministic shard in [0, 3).
	_, err = client.Increment(ctx, &pb.IncrementRequest{Delta: 1, CounterName: "post_1"})
	if err != nil {
		t.Fatalf("increment post_1: %v", err)
	}
	_, err = client.Increment(ctx, &pb.IncrementRequest{Delta: 1, CounterName: "post_2"})
	if err != nil {
		t.Fatalf("increment post_2: %v", err)
	}

	resp, err := client.ListCounters(ctx, &pb.ListCountersRequest{})
	if err != nil {
		t.Fatalf("list counters: %v", err)
	}

	shards := make(map[string]uint32)
	for _, ci := range resp.Counters {
		if ci.Name == "post_1" || ci.Name == "post_2" {
			shards[ci.Name] = ci.Shard
			if ci.Shard >= 3 {
				t.Fatalf("shard %d out of range for %s", ci.Shard, ci.Name)
			}
		}
	}

	// Assignment must be identical on every other node.
	for i := 1; i < len(c.Nodes); i++ {
		other, closeOther, err := c.Client(ctx, i)
		if err != nil {
			t.Fatalf("dial node %d: %v", i, err)
		}
		oResp, err := other.ListCounters(ctx, &pb.ListCountersRequest{})
		closeOther()
		if err != nil {
			t.Fatalf("list counters on node %d: %v", i, err)
		}
		for _, ci := range oResp.Counters {
			if ci.Name == "post_1" || ci.Name == "post_2" {
				if ci.Shard != shards[ci.Name] {
					t.Fatalf("shard mismatch for %s: node 0=%d node %d=%d",
						ci.Name, shards[ci.Name], i, ci.Shard)
				}
			}
		}
	}

	t.Logf("shard assignment: post_1=%d post_2=%d", shards["post_1"], shards["post_2"])
}

// TestManyCountersAcrossNodes exercises many counters with concurrent writers
// and verifies every counter converges to its exact total.
func TestManyCountersAcrossNodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	c := harness.Start(t, 3, harness.Options{
		GossipInterval: 200 * time.Millisecond,
	})

	const counters = 8
	const perNode = 25

	totals := make([]int64, counters)
	for i := 0; i < counters; i++ {
		name := fmt.Sprintf("post_%d", i+1)
		for n := 0; n < len(c.Nodes); n++ {
			for k := 0; k < perNode; k++ {
				incr(ctx, t, c, n, name, 1)
			}
		}
		totals[i] = int64(len(c.Nodes)) * perNode
		waitConverged(t, c, name, totals[i], timeout)
	}

	for i := 0; i < counters; i++ {
		name := fmt.Sprintf("post_%d", i+1)
		for _, v := range counterValues(c, name) {
			if v != totals[i] {
				t.Fatalf("%s = %d, want %d", name, v, totals[i])
			}
		}
	}
}
