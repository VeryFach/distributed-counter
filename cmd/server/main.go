package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/VeryFach/distributed-counter/internal/config"
	"github.com/VeryFach/distributed-counter/internal/gossip"
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

	zlog.Info("Starting Distributed Counter",
		zap.String("node_id", cfg.NodeID),
		zap.Int("grpc_port", cfg.GRPCPort))

	// Create service
	counterSvc := service.NewCounterService(cfg.NodeID, cfg.GRPCPort, zlog)

	// Create gossip engine
	gossipEngine := gossip.NewGossipEngine(cfg.NodeID, zlog)

	go gossipEngine.Start()

	// Create gRPC server
	grpcServer := server.NewGRPCServer(cfg.GRPCPort, counterSvc, gossipEngine)

	// Handle graceful shutdown
	done := make(chan bool, 1)
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		zlog.Info("Received shutdown signal, stopping server...")
		grpcServer.Stop()
		done <- true
	}()

	// Start server
	if err := grpcServer.Start(); err != nil {
		zlog.Fatal("Failed to start server", zap.Error(err))
	}

	<-done
	zlog.Info("Server stopped")
}
