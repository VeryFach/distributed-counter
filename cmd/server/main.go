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
	"github.com/VeryFach/distributed-counter/internal/crdt"
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
	staleTimeout := time.Duration(cfg.HeartbeatTimeout) * time.Second

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

	membership.SetRecovering(true)
	metrics.SetRecoveryInProgress(cfg.NodeID, true)
	if err := recoverStateFromSeedNodes(
		cfg.NodeID,
		localAddress,
		cfg.SeedNodes,
		membership,
		counterSvc,
		zlog,
	); err != nil {
		zlog.Warn("Cluster recovery completed without state sync", zap.Error(err))
	}
	membership.SetRecovering(false)
	metrics.SetRecoveryInProgress(cfg.NodeID, false)

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

func recoverStateFromSeedNodes(
	nodeID string,
	localAddress string,
	seedNodes []string,
	membership *cluster.Membership,
	counterSvc *service.CounterService,
	logger *zap.Logger,
) error {
	var lastErr error
	const (
		maxAttempts        = 5
		initialBackoff     = 250 * time.Millisecond
		maxSeedDialTimeout = 2 * time.Second
	)

	uniqueSeeds := uniqueOrderedSeeds(seedNodes)
	if len(uniqueSeeds) == 0 {
		return fmt.Errorf("no seed nodes configured for recovery")
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		startIndex := attempt % len(uniqueSeeds)
		orderedSeeds := rotateSeeds(uniqueSeeds, startIndex)
		lastFailedSeed := ""

		for _, seedAddr := range orderedSeeds {
			syncStartedAt := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), maxSeedDialTimeout)
			conn, err := grpc.DialContext(
				ctx,
				seedAddr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			)
			if err != nil {
				cancel()
				lastErr = err
				logger.Debug("Recovery seed unreachable", zap.String("seed", seedAddr), zap.Int("attempt", attempt+1), zap.Error(err))
				metrics.IncRecoverySeedFailure(nodeID, seedAddr)
				lastFailedSeed = seedAddr
				continue
			}
			cancel()

			client := pb.NewCounterServiceClient(conn)
			syncCtx, syncCancel := context.WithTimeout(context.Background(), 5*time.Second)
			stream, err := client.SyncState(syncCtx)
			if err != nil {
				syncCancel()
				lastErr = err
				logger.Debug("Recovery stream failed", zap.String("seed", seedAddr), zap.Int("attempt", attempt+1), zap.Error(err))
				metrics.IncRecoverySeedFailure(nodeID, seedAddr)
				lastFailedSeed = seedAddr
				_ = conn.Close()
				continue
			}

			if err := stream.Send(&pb.StateUpdate{
				FromNodeId:    nodeID,
				PositiveState: counterSvc.Counter().Positive(),
				NegativeState: counterSvc.Counter().Negative(),
				VectorClock:   counterSvc.Clock().State(),
				Timestamp:     time.Now().Unix(),
				Type:          pb.StateUpdate_FULL_STATE,
			}); err != nil {
				syncCancel()
				lastErr = err
				logger.Debug("Recovery send failed", zap.String("seed", seedAddr), zap.Int("attempt", attempt+1), zap.Error(err))
				metrics.IncRecoverySeedFailure(nodeID, seedAddr)
				lastFailedSeed = seedAddr
				_ = conn.Close()
				continue
			}

			response, err := stream.Recv()
			if err != nil {
				syncCancel()
				lastErr = err
				logger.Debug("Recovery receive failed", zap.String("seed", seedAddr), zap.Int("attempt", attempt+1), zap.Error(err))
				metrics.IncRecoverySeedFailure(nodeID, seedAddr)
				lastFailedSeed = seedAddr
				_ = conn.Close()
				continue
			}

			if len(response.PositiveState) > 0 || len(response.NegativeState) > 0 || len(response.VectorClock) > 0 {
				remote := pbToPNCounter(nodeID, response.PositiveState, response.NegativeState)
				counterSvc.Counter().Merge(remote)
				counterSvc.Clock().MergeMap(response.VectorClock)
				membership.AddOrUpdateMember(nodeID, localAddress, true, time.Now())
				metrics.UpdateCounterValue(nodeID, counterSvc.Counter().Value())
				metrics.ObserveRecoverySyncDuration(nodeID, seedAddr, time.Since(syncStartedAt).Seconds())
				logger.Info("Recovered state from seed", zap.String("seed", seedAddr), zap.String("node", localAddress), zap.Int("attempt", attempt+1))
				syncCancel()
				_ = conn.Close()
				return nil
			}

			syncCancel()
			_ = conn.Close()
		}

		if attempt < maxAttempts-1 {
			backoff := initialBackoff * time.Duration(1<<attempt)
			if backoff > 3*time.Second {
				backoff = 3 * time.Second
			}
			logger.Debug("Recovery retry backoff", zap.Duration("sleep", backoff), zap.Int("attempt", attempt+1))
			if lastFailedSeed != "" {
				metrics.IncRecoveryRetry(nodeID, lastFailedSeed)
			}
			time.Sleep(backoff)
		}
	}

	return lastErr
}

func uniqueOrderedSeeds(seedNodes []string) []string {
	seen := make(map[string]struct{})
	ordered := make([]string, 0, len(seedNodes))
	for _, seed := range seedNodes {
		if _, exists := seen[seed]; exists {
			continue
		}
		seen[seed] = struct{}{}
		ordered = append(ordered, seed)
	}
	return ordered
}

func rotateSeeds(seedNodes []string, offset int) []string {
	if len(seedNodes) == 0 {
		return nil
	}
	rotated := make([]string, 0, len(seedNodes))
	for i := 0; i < len(seedNodes); i++ {
		rotated = append(rotated, seedNodes[(offset+i)%len(seedNodes)])
	}
	return rotated
}

func pbToPNCounter(nodeID string, positive, negative map[string]int64) *crdt.PNCounter {
	remote := crdt.NewPNCounter(nodeID)
	remote.SetPositive(positive)
	remote.SetNegative(negative)
	return remote
}
