package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestRateLimiter(t *testing.T) {
	rl := NewRateLimiter(2)

	assert.True(t, rl.Allow())
	assert.True(t, rl.Allow())
	assert.False(t, rl.Allow())
}

func TestRateLimiterUnlimited(t *testing.T) {
	rl := NewRateLimiter(0)

	for i := 0; i < 100; i++ {
		assert.True(t, rl.Allow())
	}
}

func TestRateLimiterRefills(t *testing.T) {
	rl := NewRateLimiter(100)

	for i := 0; i < 100; i++ {
		rl.Allow()
	}
	assert.False(t, rl.Allow())

	// The token bucket only refills over time; since we just drained it,
	// a near-instant second call must still be rejected.
	assert.False(t, rl.Allow())
}

func TestAuthInterceptorAcceptsValidKey(t *testing.T) {
	interceptor := NewAuthInterceptor(MiddlewareConfig{
		AuthEnabled: true,
		APIKey:      "secret",
	})

	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("authorization", "Bearer secret"),
	)

	_, err := interceptor.Unary()(ctx, nil, &grpc.UnaryServerInfo{
		FullMethod: "/counter.CounterService/Increment",
	}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	})

	assert.NoError(t, err)
}

func TestAuthInterceptorRejectsInvalidKey(t *testing.T) {
	interceptor := NewAuthInterceptor(MiddlewareConfig{
		AuthEnabled: true,
		APIKey:      "secret",
	})

	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("authorization", "Bearer wrong"),
	)

	_, err := interceptor.Unary()(ctx, nil, &grpc.UnaryServerInfo{
		FullMethod: "/counter.CounterService/Increment",
	}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	})

	assert.Error(t, err)
}

func TestAuthInterceptorRejectsMissingKey(t *testing.T) {
	interceptor := NewAuthInterceptor(MiddlewareConfig{
		AuthEnabled: true,
		APIKey:      "secret",
	})

	_, err := interceptor.Unary()(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/counter.CounterService/Increment",
	}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	})

	assert.Error(t, err)
}

func TestAuthInterceptorDisabledAllowsAll(t *testing.T) {
	interceptor := NewAuthInterceptor(MiddlewareConfig{
		AuthEnabled: false,
		APIKey:      "",
	})

	_, err := interceptor.Unary()(context.Background(), nil, &grpc.UnaryServerInfo{
		FullMethod: "/counter.CounterService/Increment",
	}, func(ctx context.Context, req interface{}) (interface{}, error) {
		return "ok", nil
	})

	assert.NoError(t, err)
}