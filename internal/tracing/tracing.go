package tracing

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc/stats"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
)

// Config controls the OpenTelemetry tracer setup.
type Config struct {
	Enabled     bool
	Endpoint    string // OTLP gRPC collector address, e.g. jaeger:4317
	ServiceName string
	NodeID      string
	SampleRatio float64
}

// Init sets up the global OpenTelemetry tracer provider that exports spans
// to an OTLP gRPC collector (e.g. Jaeger). It installs the trace-context
// propagator so a single request is tracked end-to-end across nodes.
//
// When tracing is disabled the global provider stays the SDK noop tracer,
// so instrumentation in the gRPC layers degrades to zero-overhead no-op
// spans. The returned shutdown function must be called on process exit to
// flush pending spans.
func Init(cfg Config, logger *zap.Logger) (func(context.Context) error, error) {
	if !cfg.Enabled {
		return func(context.Context) error { return nil }, nil
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = "localhost:4317"
	}
	if cfg.SampleRatio <= 0 || cfg.SampleRatio > 1 {
		cfg.SampleRatio = 1.0
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "distributed-counter"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exp, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.Endpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			attribute.String("service.name", cfg.ServiceName),
			attribute.String("node.id", cfg.NodeID),
		),
	)
	if err != nil {
		_ = exp.Shutdown(ctx)
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(cfg.SampleRatio)),
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	logger.Info("Tracing enabled",
		zap.String("endpoint", cfg.Endpoint),
		zap.String("service_name", cfg.ServiceName),
		zap.Float64("sample_ratio", cfg.SampleRatio),
	)

	return tp.Shutdown, nil
}

// ServerHandler returns the gRPC stats handler that creates spans for
// incoming RPCs. It binds to the global tracer provider at RPC time, so it
// is safe to install unconditionally: without tracing enabled every span is
// a no-op.
func ServerHandler() stats.Handler {
	return otelgrpc.NewServerHandler()
}

// ClientHandler returns the gRPC stats handler that creates spans for
// outgoing RPCs and propagates the trace context to the peer.
func ClientHandler() stats.Handler {
	return otelgrpc.NewClientHandler()
}