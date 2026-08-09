package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/cluster"
	"github.com/VeryFach/distributed-counter/internal/config"
	"github.com/VeryFach/distributed-counter/internal/gossip"
	"github.com/VeryFach/distributed-counter/internal/metrics"
	"github.com/VeryFach/distributed-counter/internal/server"
	"github.com/VeryFach/distributed-counter/internal/service"
	"github.com/VeryFach/distributed-counter/pkg/logger"
)

func main() {
	// Parse command line flags
	var configPath string
	flag.StringVar(&configPath, "config", "configs/config.yaml", "Path to config file")
	flag.Parse()

	// Load configuration
	cfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Initialize logger
	zlog, err := logger.New()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}

	defer zlog.Sync()

	zlog.Info(
		"Starting Distributed Counter",
		zap.String("node_id", cfg.NodeID),
		zap.Int("grpc_port", cfg.GRPCPort),
	)

	metrics.StartServer(
		cfg.MetricsPort,
	)

	heartbeatInterval := time.Duration(cfg.HeartbeatInterval) * time.Second
	if heartbeatInterval <= 0 {
		heartbeatInterval = 3 * time.Second
	}
	staleTimeout := 10 * time.Second

	localAddress := fmt.Sprintf("localhost:%d", cfg.GRPCPort)

	// Create service
	counterSvc := service.NewCounterService(cfg.NodeID, cfg.GRPCPort, zlog)

	membership := cluster.NewMembership(
		cfg.NodeID,
	)
	membership.AddMember(cfg.NodeID, localAddress)

	counterSvc.SetCluster(membership)

	gossipEngine := gossip.NewGossipEngine(
		cfg.NodeID,
		counterSvc.Counter(),
		counterSvc.Clock(),
		membership,
		zlog,
	)

	// Create gRPC server
	grpcServer := server.NewGRPCServer(cfg.GRPCPort, counterSvc, gossipEngine)
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- grpcServer.Start()
	}()

	if err := waitForLocalServer(localAddress); err != nil {
		zlog.Warn("Local gRPC server did not become ready before bootstrap", zap.Error(err))
	}

	if err := joinCluster(cfg.NodeID, localAddress, cfg.SeedNodes, membership, zlog); err != nil {
		zlog.Warn("Cluster bootstrap completed with no reachable seed nodes", zap.Error(err))
	}

	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	defer runtimeCancel()

	go runHeartbeatLoop(runtimeCtx, cfg.NodeID, localAddress, heartbeatInterval, membership, zlog)
	go runFailureDetectionLoop(runtimeCtx, staleTimeout, membership, zlog)

	go gossipEngine.Start()

	// Handle graceful shutdown
	done := make(chan bool, 1)
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		zlog.Info("Received shutdown signal, stopping server...")
		runtimeCancel()
		gossipEngine.Stop()
		grpcServer.Stop()
		done <- true
	}()

	zlog.Info(
		"members",
		zap.Any(
			"members",
			membership.GetMembers(),
		),
	)

	<-done

	select {
	case err := <-serverErrCh:
		if err != nil {
			zlog.Warn("gRPC server exited", zap.Error(err))
		}
	default:
	}

	zlog.Info("Server stopped")
}

func waitForLocalServer(address string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	return nil
}

func joinCluster(
	nodeID string,
	localAddress string,
	seedNodes []string,
	membership *cluster.Membership,
	logger *zap.Logger,
) error {
	var lastErr error

	for _, seedAddr := range seedNodes {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := grpc.DialContext(
			ctx,
			seedAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
		)
		if err != nil {
			cancel()
			lastErr = err
			logger.Debug("Seed node unreachable", zap.String("seed", seedAddr), zap.Error(err))
			continue
		}
		cancel()

		client := pb.NewCounterServiceClient(conn)
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 5*time.Second)
		stream, err := client.JoinCluster(joinCtx, &pb.JoinRequest{
			NodeId:      nodeID,
			Address:     localAddress,
			StartupTime: time.Now().Unix(),
		})
		if err != nil {
			joinCancel()
			lastErr = err
			logger.Debug("JoinCluster request failed", zap.String("seed", seedAddr), zap.Error(err))
			_ = conn.Close()
			continue
		}

		joined := false
		for {
			memberList, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				lastErr = err
				logger.Debug("JoinCluster stream failed", zap.String("seed", seedAddr), zap.Error(err))
				break
			}

			for _, member := range memberList.Members {
				membership.AddOrUpdateMember(
					member.NodeId,
					member.Address,
					member.IsActive,
					time.Unix(member.LastHeartbeat, 0),
				)
			}
			joined = true
		}
		joinCancel()

		_ = conn.Close()
		if joined {
			logger.Info("Joined cluster", zap.String("seed", seedAddr))
			return nil
		}
	}

	return lastErr
}

func runHeartbeatLoop(
	ctx context.Context,
	nodeID string,
	localAddress string,
	interval time.Duration,
	membership *cluster.Membership,
	logger *zap.Logger,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sendHeartbeats(nodeID, localAddress, membership, logger)
		}
	}
}

func sendHeartbeats(
	nodeID string,
	localAddress string,
	membership *cluster.Membership,
	logger *zap.Logger,
) {
	for _, member := range membership.GetMembers() {
		if member.ID == nodeID || member.Address == localAddress {
			continue
		}

		if err := pingHeartbeat(member.Address, nodeID, logger); err != nil {
			membership.MarkInactive(member.ID)
			logger.Debug("Heartbeat failed", zap.String("member", member.ID), zap.Error(err))
			continue
		}

		membership.UpdateHeartbeat(member.ID)
	}
}

func pingHeartbeat(address, nodeID string, logger *zap.Logger) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewCounterServiceClient(conn)
	resp, err := client.Heartbeat(ctx, &pb.HeartbeatRequest{
		NodeId:    nodeID,
		Timestamp: time.Now().Unix(),
	})
	if err != nil {
		return err
	}

	logger.Debug(
		"Heartbeat acknowledged",
		zap.String("peer", address),
		zap.Int64("cluster_size", resp.ClusterSize),
	)

	return nil
}

func runFailureDetectionLoop(
	ctx context.Context,
	threshold time.Duration,
	membership *cluster.Membership,
	logger *zap.Logger,
) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			staleMembers := membership.MarkStale(threshold)
			for _, memberID := range staleMembers {
				logger.Warn("Node marked inactive", zap.String("member", memberID))
			}
		}
	}
}
