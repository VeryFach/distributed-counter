package gossip

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/VeryFach/distributed-counter/api/proto"
)

type Client struct {
	nodeID	string
	logger	*zap.Logger
	conn	*grpc.ClientConn
	client	pb.CounterServiceClient
}

func NewClient(nodeID, target string, logger *zap.Logger) (*Client, error) {
	conn, err := grpc.Dial(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, err
	}
	return &Client{
		nodeID: nodeID,
		logger: logger,
		conn: conn,
		client: pb.NewCounterServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) SendStateUpdate(ctx context.Context, update *pb.StateUpdate) error {
	return nil
	// Use bidirectional streaming? For simplicity, we can use unary or separate method.
    // But we have SyncState bidirectional streaming defined in proto.
    // For client, we need to open a stream.
    // This will be implemented in the handler/engine.
}