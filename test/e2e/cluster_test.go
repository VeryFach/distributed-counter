package e2e

import (
	"context"
	"testing"
	"time"
	"sync"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	counter "github.com/VeryFach/distributed-counter/api/proto"
)

// TestEndToEnd mensimulasikan skenario end-to-end dengan 3 node.
func TestEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Koneksi ke 3 node
	ports := []string{"50051", "50052", "50053"}
	clients := make([]counter.CounterServiceClient, 3)
	for i, port := range ports {
		conn, err := grpc.DialContext(ctx, "localhost:"+port,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock())
		require.NoError(t, err)
		defer conn.Close()
		clients[i] = counter.NewCounterServiceClient(conn)
	}

	// Lakukan serangkaian operasi
	t.Run("Increment from node A", func(t *testing.T) {
		_, err := clients[0].Increment(ctx, &counter.IncrementRequest{Delta: 10})
		require.NoError(t, err)
	})

	// Tunggu propagasi
	time.Sleep(5 * time.Second)

	t.Run("Check consistency", func(t *testing.T) {
		vals := make([]int64, 3)
		for i := 0; i < 3; i++ {
			resp, err := clients[i].GetValue(ctx, &counter.GetValueRequest{})
			require.NoError(t, err)
			vals[i] = resp.CurrentValue
		}
		// Semua node harus punya nilai yang sama (eventual consistency)
		require.Equal(t, vals[0], vals[1], "node A dan B tidak sinkron")
		require.Equal(t, vals[0], vals[2], "node A dan C tidak sinkron")
	})

	t.Run("Concurrent updates", func(t *testing.T) {
		// Jalankan update paralel
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			clients[0].Increment(ctx, &counter.IncrementRequest{Delta: 5})
		}()
		go func() {
			defer wg.Done()
			clients[1].Decrement(ctx, &counter.DecrementRequest{Delta: 3})
		}()
		go func() {
			defer wg.Done()
			clients[2].Increment(ctx, &counter.IncrementRequest{Delta: 7})
		}()
		wg.Wait()

		// Tunggu propagasi
		time.Sleep(5 * time.Second)

		// Verifikasi semua node konsisten (nilai 10+5-3+7 = 19)
		resp0, _ := clients[0].GetValue(ctx, &counter.GetValueRequest{})
		resp1, _ := clients[1].GetValue(ctx, &counter.GetValueRequest{})
		resp2, _ := clients[2].GetValue(ctx, &counter.GetValueRequest{})
		require.Equal(t, int64(19), resp0.CurrentValue)
		require.Equal(t, resp0.CurrentValue, resp1.CurrentValue)
		require.Equal(t, resp0.CurrentValue, resp2.CurrentValue)
	})
}