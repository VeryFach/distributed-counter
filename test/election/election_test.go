// Package election verifies Bully leader election: the live node with the
// highest priority becomes the coordinator, leadership is stable, and the
// cluster elects a new leader when the current one fails.
package election

import (
	"context"
	"testing"
	"time"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/test/harness"
)

const electionTimeout = 15 * time.Second

// waitLeader polls every node until they all agree on the wanted leader.
func waitLeader(t *testing.T, c *harness.Cluster, want string, d time.Duration) {
	t.Helper()

	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		var seen string
		all := true
		for _, node := range c.Nodes {
			if node.Bully == nil || !node.Running() {
				continue
			}
			leader := node.Bully.LeaderID()
			if leader == "" {
				all = false
				break
			}
			if seen == "" {
				seen = leader
			} else if seen != leader {
				all = false
				break
			}
		}

		if all && seen == want {
			t.Logf("running nodes agree on leader %q", want)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	leaders := make(map[string]int)
	for _, node := range c.Nodes {
		if node.Bully != nil {
			leaders[node.Bully.LeaderID()]++
		}
	}
	t.Fatalf("no consensus on leader %q: %v", want, leaders)
}

func TestElectionPicksHighestPriority(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// node-2 has the highest priority (3), so it must win.
	c := harness.Start(t, 3, harness.Options{
		Priorities: []int64{1, 2, 3},
	})
	c.StartLeaderElection(ctx)

	waitLeader(t, c, "node-2", electionTimeout)

	if !c.Nodes[2].Bully.IsLeader() {
		t.Fatal("node-2 does not believe it is leader")
	}
	for _, node := range c.Nodes {
		if node.Bully.IsLeader() && node.ID != "node-2" {
			t.Fatalf("%s believes it is leader, want node-2", node.ID)
		}
	}

	// The RPC surface must report the same leader.
	client, closeConn, err := c.Client(ctx, 0)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer closeConn()
	resp, err := client.GetNodeInfo(ctx, &pb.GetNodeInfoRequest{})
	if err != nil {
		t.Fatalf("GetNodeInfo: %v", err)
	}
	if resp.LeaderId != "node-2" {
		t.Fatalf("GetNodeInfo.LeaderId = %q, want node-2", resp.LeaderId)
	}
}

func TestElectionFailsOverWhenLeaderDies(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Highest priority first, then a clear runner-up.
	c := harness.Start(t, 3, harness.Options{
		Priorities: []int64{1, 2, 3},
	})
	c.StartLeaderElection(ctx)

	waitLeader(t, c, "node-2", electionTimeout)

	// Kill the leader. The cluster must elect node-1 (next highest priority).
	c.StopNode(2)
	waitLeader(t, c, "node-1", electionTimeout)
}

func TestElectionStableWithoutFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := harness.Start(t, 5, harness.Options{
		Priorities: []int64{5, 4, 3, 2, 1},
	})
	c.StartLeaderElection(ctx)

	// Highest priority is node-0.
	waitLeader(t, c, "node-0", electionTimeout)

	// Leadership must not flip while the cluster is healthy.
	time.Sleep(2 * time.Second)
	for _, node := range c.Nodes {
		if got := node.Bully.LeaderID(); got != "node-0" {
			t.Fatalf("leadership flipped to %q, want node-0", got)
		}
	}
}
