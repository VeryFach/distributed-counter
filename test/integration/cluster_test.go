package integration

import (
	"context"
	"sync"
	"testing"
	"time"

	counter "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

func waitForNode(t *testing.T, ctx context.Context, addr string) *grpc.ClientConn {
	t.Helper()
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for node %s", addr)
			return nil
		default:
			// Dial dengan timeout 5 detik, tanpa WithBlock agar tidak hang
			conn, err := grpc.Dial(addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithTimeout(5*time.Second))
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			// Tunggu koneksi siap (state READY)
			ctxWait, cancel := context.WithTimeout(ctx, 3*time.Second)
			state := conn.GetState()
			for state != connectivity.Ready && state != connectivity.Idle {
				if !conn.WaitForStateChange(ctxWait, state) {
					break
				}
				state = conn.GetState()
			}
			cancel()

			if state == connectivity.Ready || state == connectivity.Idle {
				// Test dengan GetValue (lebih sederhana)
				client := counter.NewCounterServiceClient(conn)
				testCtx, testCancel := context.WithTimeout(ctx, 2*time.Second)
				_, err := client.GetValue(testCtx, &counter.GetValueRequest{})
				testCancel()
				if err == nil {
					return conn
				}
				conn.Close()
			} else {
				conn.Close()
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
}

func TestClusterConsistency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Gunakan "localhost" agar sama dengan grpcurl
	ports := []string{"50051", "50052", "50053"}
	conns := make([]*grpc.ClientConn, 3)
	clients := make([]counter.CounterServiceClient, 3)

	t.Log("Waiting for nodes to be ready...")
	for i, port := range ports {
		addr := "localhost:" + port
		conn := waitForNode(t, ctx, addr)
		conns[i] = conn
		clients[i] = counter.NewCounterServiceClient(conn)
		t.Logf("Node %d connected", i+1)
	}
	t.Cleanup(func() {
		for _, conn := range conns {
			if conn != nil {
				_ = conn.Close()
			}
		}
	})

	// Increment on Node A
	incReq := &counter.IncrementRequest{Delta: 10}
	resp, err := clients[0].Increment(ctx, incReq)
	require.NoError(t, err, "Increment failed")
	assert.GreaterOrEqual(t, resp.CurrentValue, int64(10))

	// Wait for gossip propagation
	deadline := time.Now().Add(15 * time.Second)
	for {
		values := make([]int64, 0, 3)
		consistent := true

		for i := 0; i < 3; i++ {
			getReq := &counter.GetValueRequest{}
			resp, err := clients[i].GetValue(ctx, getReq)
			require.NoError(t, err, "GetValue failed for node %d", i)
			values = append(values, resp.CurrentValue)
		}

		for i := 1; i < len(values); i++ {
			if values[i] != values[0] {
				consistent = false
				break
			}
		}

		if consistent {
			t.Logf("Values converged: %v", values)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cluster values did not converge: %v", values)
		}
		time.Sleep(1 * time.Second)
	}
}

func TestConcurrentUpdates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ports := []string{"50051", "50052", "50053"}
	conns := make([]*grpc.ClientConn, 3)
	clients := make([]counter.CounterServiceClient, 3)

	t.Log("Waiting for nodes to be ready...")
	for i, port := range ports {
		addr := "localhost:" + port
		conn := waitForNode(t, ctx, addr)
		conns[i] = conn
		clients[i] = counter.NewCounterServiceClient(conn)
		t.Logf("Node %d connected", i+1)
	}
	t.Cleanup(func() {
		for _, conn := range conns {
			if conn != nil {
				_ = conn.Close()
			}
		}
	})

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		_, err := clients[0].Increment(ctx, &counter.IncrementRequest{Delta: 5})
		assert.NoError(t, err)
	}()
	go func() {
		defer wg.Done()
		_, err := clients[1].Decrement(ctx, &counter.DecrementRequest{Delta: 3})
		assert.NoError(t, err)
	}()
	go func() {
		defer wg.Done()
		_, err := clients[2].Increment(ctx, &counter.IncrementRequest{Delta: 7})
		assert.NoError(t, err)
	}()
	wg.Wait()

	expected := int64(9)
	deadline := time.Now().Add(15 * time.Second)
	for {
		values := make([]int64, 0, 3)
		allEqual := true

		for i := 0; i < 3; i++ {
			resp, err := clients[i].GetValue(ctx, &counter.GetValueRequest{})
			require.NoError(t, err)
			values = append(values, resp.CurrentValue)
		}

		for i := 1; i < len(values); i++ {
			if values[i] != values[0] || values[0] != expected {
				allEqual = false
				break
			}
		}

		if allEqual {
			t.Logf("Cluster converged to value %d", expected)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cluster did not converge, values: %v", values)
		}
		time.Sleep(500 * time.Millisecond)
	}
}