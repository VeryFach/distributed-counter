package integration

import (
    "context"
    "sync"
    "testing"
    "time"
    
    "github.com/stretchr/testify/assert"
    "github.com/VeryFach/distributed-counter/api/proto"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
)

func TestClusterConsistency(t *testing.T) {
    // Start 3 nodes in Docker Compose
    // This test assumes nodes are running
    
    ctx := context.Background()
    
    // Connect to each node
    conns := make([]*grpc.ClientConn, 3)
    clients := make([]counter.CounterServiceClient, 3)
    
    ports := []string{"50051", "50052", "50053"}
    for i, port := range ports {
        conn, err := grpc.Dial("localhost:"+port,
            grpc.WithTransportCredentials(insecure.NewCredentials()))
        assert.NoError(t, err)
        conns[i] = conn
        clients[i] = counter.NewCounterServiceClient(conn)
    }
    
    // Increment on Node A
    incReq := &counter.IncrementRequest{Delta: 10}
    resp, err := clients[0].Increment(ctx, incReq)
    assert.NoError(t, err)
    assert.GreaterOrEqual(t, resp.CurrentValue, int64(10))
    
    // Wait for gossip propagation
    time.Sleep(2 * time.Second)
    
    // Check all nodes have the same value
    var values []int64
    for i := 0; i < 3; i++ {
        getReq := &counter.GetValueRequest{}
        resp, err := clients[i].GetValue(ctx, getReq)
        assert.NoError(t, err)
        values = append(values, resp.CurrentValue)
    }
    
    // All values should be the same (eventual consistency)
    for i := 1; i < len(values); i++ {
        assert.Equal(t, values[0], values[i])
    }
    
    // Cleanup
    for _, conn := range conns {
        conn.Close()
    }
}

func TestConcurrentUpdates(t *testing.T) {
    // Test concurrent updates from multiple nodes
    // ...
}