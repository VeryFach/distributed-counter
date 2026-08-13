package config

import (
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	NodeID             string   `mapstructure:"node_id"`
	GRPCPort           int      `mapstructure:"grpc_port"`
	MetricsPort        int      `mapstructure:"metrics_port"`
	HTTPPort           int      `mapstructure:"http_port"`
	AdvertiseAddress   string   `mapstructure:"advertise_address"`
	SeedNodes          []string `mapstructure:"seed_nodes"`
	GossipInterval     int      `mapstructure:"gossip_interval"`
	HeartbeatInterval  int      `mapstructure:"heartbeat_interval"`
	HeartbeatTimeout   int      `mapstructure:"heartbeat_timeout"`
	PersistenceEnabled bool     `mapstructure:"persistence_enabled"`
	RedisAddr          string   `mapstructure:"redis_addr"`
	RedisPassword      string   `mapstructure:"redis_password"`
	RedisDB            int      `mapstructure:"redis_db"`

	// Phase 4: Write-Ahead Log & periodic snapshots.
	WALEnabled              bool   `mapstructure:"wal_enabled"`
	WALDir                  string `mapstructure:"wal_dir"`
	SnapshotIntervalSeconds int    `mapstructure:"snapshot_interval_seconds"`

	// Phase 3: SWIM failure detection.
	SwimInterval        int `mapstructure:"swim_interval"`
	SwimProbeTimeout    int `mapstructure:"swim_probe_timeout"`
	SwimSuspectToDead   int `mapstructure:"swim_suspect_to_dead"`

	// Phase 5: production features.
	AuthEnabled         bool   `mapstructure:"auth_enabled"`
	APIKey              string `mapstructure:"api_key"`
	RateLimitPerSecond  int    `mapstructure:"rate_limit_per_second"`
	CompressionEnabled  bool   `mapstructure:"compression_enabled"`
}

func Load(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")

	// Environment variable overrides
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	if cfg.GossipInterval <= 0 {
		cfg.GossipInterval = int((5 * time.Second).Seconds())
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = int((3 * time.Second).Seconds())
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = int((10 * time.Second).Seconds())
	}
	if cfg.SnapshotIntervalSeconds <= 0 {
		cfg.SnapshotIntervalSeconds = int((30 * time.Second).Seconds())
	}
	if cfg.SwimInterval <= 0 {
		cfg.SwimInterval = 1
	}
	if cfg.SwimProbeTimeout <= 0 {
		cfg.SwimProbeTimeout = 2
	}
	if cfg.SwimSuspectToDead <= 0 {
		cfg.SwimSuspectToDead = 3
	}
	if cfg.WALDir == "" {
		cfg.WALDir = "data"
	}

	return &cfg, nil
}
