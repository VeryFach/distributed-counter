package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	counter "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()

	ports := []string{
		"50051",
		"50052",
		"50053",
	}

	clients := make(
		[]counter.CounterServiceClient,
		3,
	)

	for i, port := range ports {
		conn, err := grpc.DialContext(
			ctx,
			"localhost:"+port,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
			grpc.WithBlock(),
		)
		require.NoError(t, err)

		defer conn.Close()

		clients[i] =
			counter.NewCounterServiceClient(conn)
	}

	// ============================
	// Ambil state awal cluster
	// ============================

	resp, err := clients[0].GetValue(
		ctx,
		&counter.GetValueRequest{},
	)
	require.NoError(t, err)

	base := resp.CurrentValue

	t.Logf(
		"Initial cluster value: %d",
		base,
	)

	// ============================
	// Increment +10
	// ============================

	t.Run("Increment from node A", func(t *testing.T) {
		_, err := clients[0].Increment(
			ctx,
			&counter.IncrementRequest{
				Delta: 10,
			},
		)

		require.NoError(t, err)
	})

	expectedAfterFirst := base + 10

	waitUntilConverged(
		t,
		ctx,
		clients,
		expectedAfterFirst,
	)

	// ============================
	// Concurrent updates
	// ============================

	t.Run("Concurrent updates", func(t *testing.T) {
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
			require.NoError(t, err)
		}()

		go func() {
			defer wg.Done()

			_, err := clients[1].Decrement(
				ctx,
				&counter.DecrementRequest{
					Delta: 3,
				},
			)
			require.NoError(t, err)
		}()

		go func() {
			defer wg.Done()

			_, err := clients[2].Increment(
				ctx,
				&counter.IncrementRequest{
					Delta: 7,
				},
			)
			require.NoError(t, err)
		}()

		wg.Wait()
	})

	expectedFinal :=
		expectedAfterFirst +
			5 -
			3 +
			7

	waitUntilConverged(
		t,
		ctx,
		clients,
		expectedFinal,
	)
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
		values := make([]int64, 0, len(clients))

		allEqual := true

		for _, c := range clients {
			resp, err := c.GetValue(
				ctx,
				&counter.GetValueRequest{},
			)
			require.NoError(t, err)

			values = append(
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

		time.Sleep(
			500 * time.Millisecond,
		)
	}
}