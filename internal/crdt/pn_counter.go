package crdt

import (
	"encoding/json"
	"sync"
)

// PNCounter implements a Positive-Negative Counter (CRDT)
type PNCounter struct {
    mu sync.RWMutex

    positive map[string]int64
    negative map[string]int64

    nodeID string
}

func NewPNCounter(nodeID string) *PNCounter {
	return &PNCounter{
		nodeID:   nodeID,
		positive: map[string]int64{},
		negative: map[string]int64{},
	}
}

func (p *PNCounter) Increment(delta int64) {
    p.mu.Lock()
    defer p.mu.Unlock()

    p.positive[p.nodeID] += delta
}

func (p *PNCounter) Decrement(delta int64) {
    p.mu.Lock()
    defer p.mu.Unlock()

    p.negative[p.nodeID] += delta
}

func (p *PNCounter) Value() int64 {
    p.mu.RLock()
    defer p.mu.RUnlock()

    var pos, neg int64

    for _, v := range p.positive {
        pos += v
    }

    for _, v := range p.negative {
        neg += v
    }

    return pos - neg
}

// Merge implements CRDT merge operation
func (p *PNCounter) Merge(other *PNCounter) {
    p.mu.Lock()
    defer p.mu.Unlock()

    other.mu.RLock()
    defer other.mu.RUnlock()

    for node, v := range other.positive {
        if v > p.positive[node] {
            p.positive[node] = v
        }
    }

    for node, v := range other.negative {
        if v > p.negative[node] {
            p.negative[node] = v
        }
    }
}

func (p *PNCounter) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"node_id":  p.nodeID,
		"positive": p.positive,
		"negative": p.negative,
		"value":    p.Value(),
	})
}

func (p *PNCounter) SetPositive(state map[string]int64) {
    p.mu.Lock()
    defer p.mu.Unlock()

    p.positive = make(map[string]int64)
    for k, v := range state {
        p.positive[k] = v
    }
}

func (p *PNCounter) SetNegative(state map[string]int64) {
    p.mu.Lock()
    defer p.mu.Unlock()

    p.negative = make(map[string]int64)
    for k, v := range state {
        p.negative[k] = v
    }
}

func (p *PNCounter) Positive() map[string]int64 {
    p.mu.RLock()
    defer p.mu.RUnlock()

    res := make(map[string]int64)
    for k, v := range p.positive {
        res[k] = v
    }
    return res
}

func (p *PNCounter) Negative() map[string]int64 {
    p.mu.RLock()
    defer p.mu.RUnlock()

    res := make(map[string]int64)
    for k, v := range p.negative {
        res[k] = v
    }
    return res
}
