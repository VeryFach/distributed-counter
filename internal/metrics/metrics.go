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