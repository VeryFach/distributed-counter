package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"go.uber.org/zap"

	"github.com/VeryFach/distributed-counter/internal/config"
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
	log, err := logger.New()
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}
	defer log.Sync()

	log.Info("Starting Distributed Counter",
		zap.String("node_id", cfg.NodeID),
		zap.Int("grpc_port", cfg.GRPCPort))

	// Create service
	counterSvc := service.NewCounterService(cfg.NodeID, log)

	// Create gRPC server
	grpcServer := server.NewGRPCServer(cfg.GRPCPort, counterSvc, log)

	// Handle graceful shutdown
	done := make(chan bool, 1)
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		log.Info("Received shutdown signal, stopping server...")
		grpcServer.Stop()
		done <- true
	}()

	// Start server
	if err := grpcServer.Start(); err != nil {
		log.Fatal("Failed to start server", zap.Error(err))
	}

	<-done
	log.Info("Server stopped")
}
