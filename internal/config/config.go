package config

import (
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	NodeID            string   `mapstructure:"node_id"`
	GRPCPort          int      `mapstructure:"grpc_port"`
	MetricsPort       int      `mapstructure:"metrics_port"`
	HTTPPort          int      `mapstructure:"http_port"`
	SeedNodes         []string `mapstructure:"seed_nodes"`
	GossipInterval    int      `mapstructure:"gossip_interval"`
	HeartbeatInterval int      `mapstructure:"heartbeat_interval"`
	HeartbeatTimeout  int      `mapstructure:"heartbeat_timeout"`
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

	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = int((3 * time.Second).Seconds())
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = int((10 * time.Second).Seconds())
	}

	return &cfg, nil
}
