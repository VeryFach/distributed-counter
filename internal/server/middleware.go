package server

import (
	"context"
	"crypto/subtle"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/VeryFach/distributed-counter/internal/metrics"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MiddlewareConfig configures the server interceptors.
type MiddlewareConfig struct {
	NodeID              string
	Logger              *zap.Logger
	AuthEnabled         bool
	APIKey              string
	RateLimitPerSecond  int
}

// AuthInterceptor validates the API key on every request when enabled.
type AuthInterceptor struct {
	nodeID  string
	logger  *zap.Logger
	enabled bool
	apiKey  string
}

func NewAuthInterceptor(cfg MiddlewareConfig) *AuthInterceptor {
	return &AuthInterceptor{
		nodeID:  cfg.NodeID,
		logger:  cfg.Logger,
		enabled: cfg.AuthEnabled,
		apiKey:  cfg.APIKey,
	}
}

// Unary returns a unary interceptor enforcing API key auth.
func (a *AuthInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if a.enabled && !a.allowlist(info.FullMethod) && !a.authorized(ctx) {
			metrics.IncAuthRejected(a.nodeID, info.FullMethod)
			return nil, status.Error(codes.Unauthenticated, "missing or invalid API key")
		}
		return handler(ctx, req)
	}
}

// Stream returns a stream interceptor enforcing API key auth.
func (a *AuthInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if a.enabled && !a.allowlist(info.FullMethod) && !a.authorized(ss.Context()) {
			metrics.IncAuthRejected(a.nodeID, info.FullMethod)
			return status.Error(codes.Unauthenticated, "missing or invalid API key")
		}
		return handler(srv, ss)
	}
}

// allowlist exempts health and reflection RPCs from auth so tooling still
// works even when the API key is required for application calls.
func (a *AuthInterceptor) allowlist(method string) bool {
	return strings.HasPrefix(method, "/grpc.health.v1.Health/") ||
		strings.HasPrefix(method, "/grpc.reflection.")
}

func (a *AuthInterceptor) authorized(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}

	values := md.Get("authorization")
	if len(values) == 0 {
		return false
	}

	token := strings.TrimPrefix(values[0], "Bearer ")
	if a.apiKey == "" {
		return false
	}

	return subtle.ConstantTimeCompare([]byte(token), []byte(a.apiKey)) == 1
}

// RateLimiter is a token bucket limiter.
type RateLimiter struct {
	mu       sync.Mutex
	tokens   float64
	capacity float64
	refill   float64
	last     time.Time
}

func NewRateLimiter(perSecond int) *RateLimiter {
	if perSecond <= 0 {
		return &RateLimiter{capacity: math.MaxFloat64, tokens: math.MaxFloat64}
	}
	return &RateLimiter{
		tokens:   float64(perSecond),
		capacity: float64(perSecond),
		refill:   float64(perSecond),
		last:     time.Now(),
	}
}

// Allow reports whether a request may proceed, consuming one token.
func (r *RateLimiter) Allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.capacity == math.MaxFloat64 {
		return true
	}

	now := time.Now()
	elapsed := now.Sub(r.last).Seconds()
	r.tokens = math.Min(r.capacity, r.tokens+elapsed*r.refill)
	r.last = now

	if r.tokens >= 1 {
		r.tokens--
		return true
	}
	return false
}

// RateLimitInterceptor rejects requests once the token bucket is empty.
type RateLimitInterceptor struct {
	nodeID string
	logger *zap.Logger
	limiter *RateLimiter
}

func NewRateLimitInterceptor(cfg MiddlewareConfig) *RateLimitInterceptor {
	return &RateLimitInterceptor{
		nodeID:  cfg.NodeID,
		logger:  cfg.Logger,
		limiter: NewRateLimiter(cfg.RateLimitPerSecond),
	}
}

func (r *RateLimitInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !r.limiter.Allow() {
			metrics.IncRateLimited(r.nodeID, info.FullMethod)
			return nil, status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}
		return handler(ctx, req)
	}
}

func (r *RateLimitInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if !r.limiter.Allow() {
			metrics.IncRateLimited(r.nodeID, info.FullMethod)
			return status.Error(codes.ResourceExhausted, "rate limit exceeded")
		}
		return handler(srv, ss)
	}
}