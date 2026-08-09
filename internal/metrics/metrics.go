package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Counter operations
	IncrementTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "counter_increment_total",
			Help: "Total number of increment operations",
		},
		[]string{"node_id"},
	)
	DecrementTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "counter_decrement_total",
			Help: "Total number of decrement operations",
		},
		[]string{"node_id"},
	)
	// Current counter value per node
	CurrentValue = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "counter_current_value",
			Help: "Current counter value",
		},
		[]string{"node_id"},
	)
	// Gossip messages
	GossipMessagesSent = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gossip_messages_sent_total",
			Help: "Total number of gossip messages sent",
		},
		[]string{"node_id"},
	)
	GossipMessagesReceived = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "gossip_messages_received_total",
			Help: "Total number of gossip messages received",
		},
		[]string{"node_id"},
	)
	// Recovery observability
	RecoveryRetryTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "recovery_retry_total",
			Help: "Total number of recovery retries",
		},
		[]string{"node_id", "seed_node"},
	)
	RecoverySeedFailuresTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "recovery_seed_failures_total",
			Help: "Total number of failed recovery attempts per seed node",
		},
		[]string{"node_id", "seed_node"},
	)
	RecoverySyncDurationSeconds = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "recovery_sync_duration_seconds",
			Help:    "Duration of successful recovery state sync",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"node_id", "seed_node"},
	)
	RecoveryInProgress = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "recovery_in_progress",
			Help: "Whether a node is currently recovering state",
		},
		[]string{"node_id"},
	)
)

func UpdateCounterValue(nodeID string, value int64) {
	CurrentValue.WithLabelValues(nodeID).Set(float64(value))
}

func IncIncrementTotal(nodeID string) {
	IncrementTotal.WithLabelValues(nodeID).Inc()
}

func IncDecrementTotal(nodeID string) {
	DecrementTotal.WithLabelValues(nodeID).Inc()
}

func IncGossipSent(nodeID string) {
	GossipMessagesSent.WithLabelValues(nodeID).Inc()
}

func IncGossipReceived(nodeID string) {
	GossipMessagesReceived.WithLabelValues(nodeID).Inc()
}

func IncRecoveryRetry(nodeID, seedNode string) {
	RecoveryRetryTotal.WithLabelValues(nodeID, seedNode).Inc()
}

func IncRecoverySeedFailure(nodeID, seedNode string) {
	RecoverySeedFailuresTotal.WithLabelValues(nodeID, seedNode).Inc()
}

func ObserveRecoverySyncDuration(nodeID, seedNode string, seconds float64) {
	RecoverySyncDurationSeconds.WithLabelValues(nodeID, seedNode).Observe(seconds)
}

func SetRecoveryInProgress(nodeID string, inProgress bool) {
	if inProgress {
		RecoveryInProgress.WithLabelValues(nodeID).Set(1)
		return
	}

	RecoveryInProgress.WithLabelValues(nodeID).Set(0)
}
