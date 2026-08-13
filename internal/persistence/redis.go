package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStore persists counter state in Redis using JSON snapshots. It is
// used so that a node that restarts can recover its positive/negative maps
// and vector clock instead of starting from zero.
type RedisStore struct {
	client *redis.Client
}

// NewRedisStore connects to a Redis server and verifies connectivity.
// It returns the Store interface so a failure produces a nil interface,
// never a typed-nil pointer hiding inside one.
func NewRedisStore(addr, password string, db int) (Store, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	return &RedisStore{client: client}, nil
}

func (r *RedisStore) stateKey(nodeID string) string {
	return "counter:" + nodeID + ":state"
}

func (r *RedisStore) Save(nodeID string, state CounterState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return r.client.Set(ctx, r.stateKey(nodeID), data, 0).Err()
}

func (r *RedisStore) Load(nodeID string) (*CounterState, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	data, err := r.client.Get(ctx, r.stateKey(nodeID)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var state CounterState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

func (r *RedisStore) Close() error {
	return r.client.Close()
}
