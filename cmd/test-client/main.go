package main

import (
	"context"
	"flag"
	"log"
	"time"

	counter "github.com/VeryFach/distributed-counter/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:50051", "server address")
	delta := flag.Int("delta", 1, "increment/decrement delta")
	decrement := flag.Bool("decrement", false, "decrement instead of increment")
	reset := flag.Bool("reset", false, "reset the counter before applying the operation")
	get := flag.Bool("get", false, "read the current value without mutating")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, *addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		log.Fatalf("❌ Dial error: %v", err)
	}
	defer conn.Close()
	log.Println("✅ Connected!")

	client := counter.NewCounterServiceClient(conn)

	if *reset {
		resp, err := client.Reset(ctx, &counter.ResetRequest{})
		if err != nil {
			log.Fatalf("❌ Reset error: %v", err)
		}
		log.Printf("✅ Reset done, value now %d", resp.CurrentValue)
		return
	}

	if *get {
		resp, err := client.GetValue(ctx, &counter.GetValueRequest{})
		if err != nil {
			log.Fatalf("❌ GetValue error: %v", err)
		}
		log.Printf("✅ Value: %d (version %s)", resp.CurrentValue, resp.Version)
		return
	}

	var resp *counter.CounterResponse
	if *decrement {
		resp, err = client.Decrement(ctx, &counter.DecrementRequest{Delta: int32(*delta)})
	} else {
		resp, err = client.Increment(ctx, &counter.IncrementRequest{Delta: int32(*delta)})
	}
	if err != nil {
		log.Fatalf("❌ Counter update error: %v", err)
	}
	log.Printf("✅ New value: %d (version %s)", resp.CurrentValue, resp.Version)

	nodeInfo, err := client.GetNodeInfo(ctx, &counter.GetNodeInfoRequest{})
	if err != nil {
		log.Fatalf("❌ GetNodeInfo error: %v", err)
	}
	log.Printf("✅ NodeInfo: %+v", nodeInfo)
}
