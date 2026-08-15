// Package benchmark measures throughput, gossip convergence time and memory
// footprint of the in-process cluster at different node counts. Run with:
//
//	go test -bench=. -benchmem ./test/benchmark/...
//
// The memory benchmark is most meaningful with a single iteration:
//
//	go test -bench=Memory -benchtime=1x ./test/benchmark/...
package benchmark

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/test/harness"
)

var nodeCounts = []int{3, 5, 10, 20}

func startCluster(b *testing.B, n int) *harness.Cluster {
	b.Helper()
	return harness.Start(b, n, harness.Options{
		GossipInterval: 200 * time.Millisecond,
	})
}

// clients dials one persistent gRPC client per node.
func clients(b *testing.B, c *harness.Cluster) []pb.CounterServiceClient {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conns := make([]*grpc.ClientConn, len(c.Nodes))
	clients := make([]pb.CounterServiceClient, len(c.Nodes))
	for i, node := range c.Nodes {
		conn, err := grpc.DialContext(
			ctx,
			node.Addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			b.Fatalf("dial node %d: %v", i, err)
		}
		conns[i] = conn
		clients[i] = pb.NewCounterServiceClient(conn)
	}
	b.Cleanup(func() {
		for _, conn := range conns {
			if conn != nil {
				_ = conn.Close()
			}
		}
	})
	return clients
}

// BenchmarkIncrementThroughput measures sustained increment operations/sec
// across the whole cluster with concurrent writers.
func BenchmarkIncrementThroughput(b *testing.B) {
	for _, n := range nodeCounts {
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			c := startCluster(b, n)
			clients := clients(b, c)

			var seq atomic.Uint32
			b.ResetTimer()
			b.RunParallel(func(pp *testing.PB) {
				for pp.Next() {
					i := int(seq.Add(1)-1) % len(clients)
					if _, err := clients[i].Increment(context.Background(), &pb.IncrementRequest{Delta: 1}); err != nil {
						b.Error(err)
						return
					}
				}
			})
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "incr/sec")
		})
	}
}

// BenchmarkConvergence issues a fixed batch of increments round-robin and
// measures how long it takes for every node's local value to converge to the
// batch total.
func BenchmarkConvergence(b *testing.B) {
	const batch = 1000

	for _, n := range nodeCounts {
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			c := startCluster(b, n)
			clients := clients(b, c)
			ctx := context.Background()

			b.ResetTimer()
			for iter := 0; iter < b.N; iter++ {
				for _, cl := range clients {
					if _, err := cl.Reset(ctx, &pb.ResetRequest{}); err != nil {
						b.Fatal(err)
					}
				}

				start := time.Now()
				for j := 0; j < batch; j++ {
					if _, err := clients[j%len(clients)].Increment(ctx, &pb.IncrementRequest{Delta: 1}); err != nil {
						b.Fatal(err)
					}
				}

				deadline := time.Now().Add(10 * time.Second)
				for {
					values := c.Values()
					converged := true
					for _, v := range values {
						if v != batch {
							converged = false
							break
						}
					}
					if converged {
						break
					}
					if time.Now().After(deadline) {
						b.Fatalf("cluster did not converge: %v", values)
					}
					time.Sleep(20 * time.Millisecond)
				}

				elapsed := time.Since(start)
				b.ReportMetric(elapsed.Seconds()*1000, "convergence_ms")
				b.ReportMetric(float64(batch)/elapsed.Seconds(), "batch_incr/sec")
			}
		})
	}
}

// BenchmarkMemoryPerNode reports the heap footprint per node after the
// cluster has been warmed up. Best run with -benchtime=1x.
func BenchmarkMemoryPerNode(b *testing.B) {
	for _, n := range nodeCounts {
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			c := startCluster(b, n)
			clients := clients(b, c)

			for i := 0; i < 100; i++ {
				if _, err := clients[i%len(clients)].Increment(context.Background(), &pb.IncrementRequest{Delta: 1}); err != nil {
					b.Fatal(err)
				}
			}

			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_ = c.Values()
			runtime.ReadMemStats(&after)

			b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/float64(n), "delta_heap_B/node")
			b.ReportMetric(float64(after.HeapAlloc)/float64(n), "total_heap_B/node")
		})
	}
}
