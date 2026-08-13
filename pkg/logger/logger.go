package logger

import (
	"os"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// New builds a production zap logger. Set COUNTER_LOG_LEVEL to debug to
// enable verbose logging (e.g. gossip round details).
func New() (*zap.Logger, error) {
	config := zap.NewProductionConfig()
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	switch strings.ToLower(os.Getenv("COUNTER_LOG_LEVEL")) {
	case "debug":
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}

	return config.Build()
}