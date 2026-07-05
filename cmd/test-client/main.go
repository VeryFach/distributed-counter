package main

import (
	"context"
	"log"
	"time"

	counter "github.com/VeryFach/distributed-counter/api/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, "127.0.0.1:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		log.Fatalf("❌ Dial error: %v", err)
	}
	defer conn.Close()
	log.Println("✅ Connected!")

	client := counter.NewCounterServiceClient(conn)
	resp, err := client.GetNodeInfo(ctx, &counter.GetNodeInfoRequest{})
	if err != nil {
		log.Fatalf("❌ GetNodeInfo error: %v", err)
	}
	log.Printf("✅ NodeInfo: %+v", resp)
}