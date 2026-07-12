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

func waitForNode(
	t *testing.T,
	ctx context.Context,
	addr string,
) *grpc.ClientConn {
	t.Helper()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting for node %s", addr)
			return nil

		default:
			conn, err := grpc.Dial(
				addr,
				grpc.WithTransportCredentials(
					insecure.NewCredentials(),
				),
				grpc.WithTimeout(5*time.Second),
			)
			if err != nil {
				time.Sleep(500 * time.Millisecond)
				continue
			}

			ctxWait, cancel :=
				context.WithTimeout(
					ctx,
					3*time.Second,
				)

			state := conn.GetState()

			for state != connectivity.Ready &&
				state != connectivity.Idle {
				if !conn.WaitForStateChange(
					ctxWait,
					state,
				) {
					break
				}

				state = conn.GetState()
			}

			cancel()

			if state == connectivity.Ready ||
				state == connectivity.Idle {

				client :=
					counter.NewCounterServiceClient(conn)

				testCtx, testCancel :=
					context.WithTimeout(
						ctx,
						2*time.Second,
					)

				_, err = client.GetValue(
					testCtx,
					&counter.GetValueRequest{},
				)

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

func createClients(
	t *testing.T,
	ctx context.Context,
) (
	[]*grpc.ClientConn,
	[]counter.CounterServiceClient,
) {
	ports := []string{
		"50051",
		"50052",
		"50053",
	}

	conns := make([]*grpc.ClientConn, 3)
	clients :=
		make([]counter.CounterServiceClient, 3)

	for i, port := range ports {
		addr := "localhost:" + port

		conn := waitForNode(
			t,
			ctx,
			addr,
		)

		conns[i] = conn
		clients[i] =
			counter.NewCounterServiceClient(conn)

		t.Logf(
			"Node %d connected",
			i+1,
		)
	}

	t.Cleanup(func() {
		for _, conn := range conns {
			if conn != nil {
				_ = conn.Close()
			}
		}
	})

	return conns, clients
}

func clusterValue(
	t *testing.T,
	ctx context.Context,
	client counter.CounterServiceClient,
) int64 {
	resp, err := client.GetValue(
		ctx,
		&counter.GetValueRequest{},
	)
	require.NoError(t, err)

	return resp.CurrentValue
}

func waitUntilConverged(
	t *testing.T,
	ctx context.Context,
	clients []counter.CounterServiceClient,
	expected int64,
) {
	deadline :=
		time.Now().Add(15 * time.Second)

	for {
		values := make(
			[]int64,
			0,
			len(clients),
		)

		allEqual := true

		for _, c := range clients {
			resp, err := c.GetValue(
				ctx,
				&counter.GetValueRequest{},
			)
			require.NoError(t, err)

			values =
				append(
					values,
					resp.CurrentValue,
				)
		}

		for i := 1; i < len(values); i++ {
			if values[i] != values[0] {
				allEqual = false
				break
			}
		}

		if allEqual &&
			values[0] == expected {
			t.Logf(
				"Cluster converged to %d",
				expected,
			)
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf(
				"cluster did not converge, expected=%d values=%v",
				expected,
				values,
			)
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func TestClusterConsistency(t *testing.T) {
	ctx, cancel :=
		context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
	defer cancel()

	t.Log("Waiting for nodes to be ready...")

	_, clients :=
		createClients(
			t,
			ctx,
		)

	base :=
		clusterValue(
			t,
			ctx,
			clients[0],
		)

	_, err := clients[0].Increment(
		ctx,
		&counter.IncrementRequest{
			Delta: 10,
		},
	)
	require.NoError(t, err)

	expected := base + 10

	waitUntilConverged(
		t,
		ctx,
		clients,
		expected,
	)
}

func TestConcurrentUpdates(t *testing.T) {
	ctx, cancel :=
		context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
	defer cancel()

	t.Log("Waiting for nodes to be ready...")

	_, clients :=
		createClients(
			t,
			ctx,
		)

	base :=
		clusterValue(
			t,
			ctx,
			clients[0],
		)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()

		_, err := clients[0].Increment(
			ctx,
			&counter.IncrementRequest{
				Delta: 5,
			},
		)

		assert.NoError(t, err)
	}()

	go func() {
		defer wg.Done()

		_, err := clients[1].Decrement(
			ctx,
			&counter.DecrementRequest{
				Delta: 3,
			},
		)

		assert.NoError(t, err)
	}()

	go func() {
		defer wg.Done()

		_, err := clients[2].Increment(
			ctx,
			&counter.IncrementRequest{
				Delta: 7,
			},
		)

		assert.NoError(t, err)
	}()

	wg.Wait()

	expected := base + 5 - 3 + 7

	waitUntilConverged(
		t,
		ctx,
		clients,
		expected,
	)
}