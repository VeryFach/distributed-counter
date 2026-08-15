// Package gateway exposes the gRPC services over HTTP/JSON using
// grpc-gateway. It translates RESTful requests (POST /v1/counter/increment,
// GET /v1/counter/value, ...) into calls on the local in-process gRPC service
// implementations.
package gateway

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.uber.org/zap"

	pb "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/internal/admin"
	"github.com/VeryFach/distributed-counter/internal/dashboard"
)

// Server serves the HTTP/JSON gateway plus the web dashboard on a single
// HTTP port.
type Server struct {
	port    int
	mux     *runtime.ServeMux
	handler http.Handler
	srv     *http.Server
	logger  *zap.Logger
}

// Options configures the gateway server.
type Options struct {
	// Port is the HTTP port the gateway listens on.
	Port int
	// AdminService is registered under /v1/admin/*.
	AdminService *admin.AdminService
	// Dashboard is the web UI; when nil the dashboard is not served.
	Dashboard *dashboard.Dashboard
	// Logger for gateway lifecycle messages.
	Logger *zap.Logger
}

// New builds a gateway server that serves the CounterService and AdminService
// over HTTP/JSON on the given port. counterSvc is the in-process gRPC service
// implementation; the gateway calls it directly (no network round-trip).
func New(counterSvc pb.CounterServiceServer, opts Options) *Server {
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	mux := runtime.NewServeMux(
		runtime.WithErrorHandler(runtime.DefaultHTTPErrorHandler),
	)

	ctx := context.Background()
	if err := pb.RegisterCounterServiceHandlerServer(ctx, mux, counterSvc); err != nil {
		logger.Warn("Failed to register counter gateway", zap.Error(err))
	}
	if opts.AdminService != nil {
		if err := pb.RegisterAdminServiceHandlerServer(ctx, mux, opts.AdminService); err != nil {
			logger.Warn("Failed to register admin gateway", zap.Error(err))
		}
	}
	if opts.Dashboard != nil {
		mux.HandlePath(http.MethodGet, "/api/cluster", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
			opts.Dashboard.Cluster(w, r)
		})
		mux.HandlePath(http.MethodGet, "/", func(w http.ResponseWriter, r *http.Request, _ map[string]string) {
			opts.Dashboard.Index(w, r)
		})
	}

	return &Server{
		port:    opts.Port,
		mux:     mux,
		handler: mux,
		logger:  logger,
	}
}

// Handler returns the root HTTP handler for the gateway (useful for tests
// that want to route the gateway under an existing mux).
func (s *Server) Handler() http.Handler {
	return s.handler
}

// Mux returns the underlying grpc-gateway mux.
func (s *Server) Mux() *runtime.ServeMux {
	return s.mux
}

// Start listens on the configured port and serves HTTP until the server is
// stopped. It blocks; call it in a goroutine.
func (s *Server) Start() error {
	addr := fmt.Sprintf(":%d", s.port)
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           s.handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	s.logger.Info("HTTP gateway listening", zap.String("addr", addr))
	return s.srv.ListenAndServe()
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop() {
	if s.srv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.srv.Shutdown(ctx)
	}
}
