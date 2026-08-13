package grpcutil

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)

// apiKeyCredentials attaches the API key as a Bearer token to every RPC so
// cluster traffic is authenticated once auth is enabled.
type apiKeyCredentials struct {
	key string
}

func (c apiKeyCredentials) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + c.key}, nil
}

func (c apiKeyCredentials) RequireTransportSecurity() bool { return false }

// DialOptions builds the shared client dial options for cluster traffic.
//   - apiKey: when non-empty, the API key is attached to every RPC so the
//     server-side auth interceptor accepts the call.
//   - compression: enables gRPC gzip so large state payloads are compressed.
func DialOptions(apiKey string, compression bool) []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if apiKey != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(apiKeyCredentials{key: apiKey}))
	}
	if compression {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.UseCompressor(gzip.Name)))
	}
	return opts
}