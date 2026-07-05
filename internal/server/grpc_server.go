package server

import (
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1" // FIX 1: Import grpc_health_v1 untuk status SERVING
	"google.golang.org/grpc/reflection"

	// FIX 2: Beri alias 'pb' untuk package proto agar mudah dipanggil
	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/gossip" // FIX 3: Tambahkan import gossip
	"github.com/VeryFach/distributed-counter/internal/service"
)

type GRPCServer struct {
	server       *grpc.Server
	port         int
	counterSvc   *service.CounterService
	gossipEngine *gossip.GossipEngine
	healthSvc    *health.Server
}

func NewGRPCServer(port int, counterSvc *service.CounterService, gossipEngine *gossip.GossipEngine) *GRPCServer {
	return &GRPCServer{
		server:       grpc.NewServer(getServerOptions()...),
		port:         port,
		counterSvc:   counterSvc,
		gossipEngine: gossipEngine,
		healthSvc:    health.NewServer(),
	}
}

func (s *GRPCServer) Start() error {
	// FIX 4: Gunakan alias 'pb' untuk memanggil Register
	pb.RegisterCounterServiceServer(s.server, s.counterSvc)
	// Gossip handler is embedded in counter service implementation

	// Register health check
	// FIX 5: Gunakan grpc_health_v1 dan daftarkan health server ke gRPC server
	s.healthSvc.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(s.server, s.healthSvc)

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
	// FIX 6: Comment sementara interceptor yang belum dibuat fungsinya
	return []grpc.ServerOption{
		/*
			grpc.ChainUnaryInterceptor(
				loggingInterceptor(),
				recoveryInterceptor(),
				metricsInterceptor(),
			),
			grpc.ChainStreamInterceptor(
				streamLoggingInterceptor(),
				streamMetricsInterceptor(),
			),
		*/
		// Max message size (useful for large state updates)
		grpc.MaxRecvMsgSize(10 * 1024 * 1024), // 10 MB
		grpc.MaxSendMsgSize(10 * 1024 * 1024),
	}
}

func (s *GRPCServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}
