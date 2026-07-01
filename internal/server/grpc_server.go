package server

import (
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/reflection"

	"github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/service"
)

type GRPCServer struct {
	server     *grpc.Server
	port       int
	counterSvc *service.CounterService
	gossipHdl  *gossip.Handler
	healthSvc  *health.Server
}

func NewGRPCServer(port int, counterSvc *service.CounterService, gossipHdl *gossip.Handler) *GRPCServer {
	return &GRPCServer{
		server:     grpc.NewServer(getServerOptions()...),
		port:       port,
		counterSvc: counterSvc,
		gossipHdl:  gossipHdl,
		healthSvc:  health.NewServer(),
	}
}

func (s *GRPCServer) Start() error {
	// Register services
	counter.RegisterCounterServiceServer(s.server, s.counterSvc)
	// Gossip handler is embedded in counter service implementation

	// Register health check
	s.healthSvc.SetServingStatus("", health.HealthCheckResponse_SERVING)

	// Enable reflection for debugging
	reflection.Register(s.server)

	// Start listening
	addr := fmt.Sprintf(":%d", s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	// Start serving
	return s.server.Serve(listener)
}

func getServerOptions() []grpc.ServerOption {
	// Add interceptors: logging, recovery, metrics, etc.
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(
			loggingInterceptor(),
			recoveryInterceptor(),
			metricsInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			streamLoggingInterceptor(),
			streamMetricsInterceptor(),
		),
		// Max message size (useful for large state updates)
		grpc.MaxRecvMsgSize(10 * 1024 * 1024), // 10 MB
		grpc.MaxSendMsgSize(10 * 1024 * 1024),
	}
}
