package crdt

import (
	"encoding/json"
	"sync"
)

// PNCounter implements a Positive-Negative Counter (CRDT)
type PNCounter struct {
	mu       sync.RWMutex
	positive int64
	negative int64
	nodeID   string
}

func NewPNCounter(nodeID string) *PNCounter {
	return &PNCounter{
		nodeID:   nodeID,
		positive: 0,
		negative: 0,
	}
}

func (p *PNCounter) Increment(delta int64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.positive += delta
	return p.Value()
}

func (p *PNCounter) Decrement(delta int64) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.negative += delta
	return p.Value()
}

func (p *PNCounter) Value() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.positive - p.negative
}

// Merge implements CRDT merge operation
func (p *PNCounter) Merge(other *PNCounter) int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	// Take the maximum of both counters
	if other.positive > p.positive {
		p.positive = other.positive
	}
	if other.negative > p.negative {
		p.negative = other.negative
	}

	return p.Value()
}

func (p *PNCounter) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"node_id":  p.nodeID,
		"positive": p.positive,
		"negative": p.negative,
		"value":    p.Value(),
	})
}
