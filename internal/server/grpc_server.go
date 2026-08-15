package server

import (
	"fmt"
	"net"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/gossip"
	"github.com/VeryFach/distributed-counter/internal/service"
	"github.com/VeryFach/distributed-counter/internal/tracing"
)

type GRPCServer struct {
	server       *grpc.Server
	port         int
	counterSvc   *service.CounterService
	gossipEngine *gossip.GossipEngine
	healthSvc    *health.Server
	adminSvc     pb.AdminServiceServer
}

func NewGRPCServer(port int, counterSvc *service.CounterService, gossipEngine *gossip.GossipEngine, cfg MiddlewareConfig, extra ...pb.AdminServiceServer) *GRPCServer {
	auth := NewAuthInterceptor(cfg)
	rateLimit := NewRateLimitInterceptor(cfg)

	unaryInterceptors := []grpc.UnaryServerInterceptor{
		auth.Unary(),
		rateLimit.Unary(),
	}
	streamInterceptors := []grpc.StreamServerInterceptor{
		auth.Stream(),
		rateLimit.Stream(),
	}

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamInterceptors...),
		// Distributed tracing: spans every RPC (no-op when tracing disabled).
		grpc.StatsHandler(tracing.ServerHandler()),
		// Large state updates (10 MB)
		grpc.MaxRecvMsgSize(10 * 1024 * 1024),
		grpc.MaxSendMsgSize(10 * 1024 * 1024),
	}

	var adminSvc pb.AdminServiceServer
	if len(extra) > 0 {
		adminSvc = extra[0]
	}

	return &GRPCServer{
		server:       grpc.NewServer(opts...),
		port:         port,
		counterSvc:   counterSvc,
		gossipEngine: gossipEngine,
		healthSvc:    health.NewServer(),
		adminSvc:     adminSvc,
	}
}

func (s *GRPCServer) Start() error {
	pb.RegisterCounterServiceServer(s.server, s.counterSvc)
	if s.adminSvc != nil {
		pb.RegisterAdminServiceServer(s.server, s.adminSvc)
	}

	s.healthSvc.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(s.server, s.healthSvc)

	reflection.Register(s.server)

	addr := fmt.Sprintf(":%d", s.port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	return s.server.Serve(listener)
}

func (s *GRPCServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}