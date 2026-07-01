package config

import (
    "github.com/spf13/viper"
)

type Config struct {
    NodeID      string   `mapstructure:"node_id"`
    GRPCPort    int      `mapstructure:"grpc_port"`
    HTTPPort    int      `mapstructure:"http_port"`
    SeedNodes   []string `mapstructure:"seed_nodes"`
    GossipInterval int   `mapstructure:"gossip_interval"`
    HeartbeatInterval int `mapstructure:"heartbeat_interval"`
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
    
    return &cfg, nil
}