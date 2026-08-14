package crdt

import (
	"encoding/json"
	"sort"
	"strings"
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

// IncrementName increments the named counter. The default counter keeps the
// bare replica id as its map key so legacy persisted state stays compatible;
// other counters use "<name>:<replica id>" keys. Because every merge below is
// max-based, all counters can safely share the same PNCounter instance.
func (p *PNCounter) IncrementName(name string, delta int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.positive[counterKey(name, p.nodeID)] += delta
}

// DecrementName decrements the named counter.
func (p *PNCounter) DecrementName(name string, delta int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.negative[counterKey(name, p.nodeID)] += delta
}

// counterKey builds the map key for a replica's contribution to a counter.
// The default counter uses the bare replica id (legacy compatible), any other
// counter is namespaced as "<name>:<replica id>".
func counterKey(name, nodeID string) string {
	if name == "" || name == "default" {
		return nodeID
	}
	return name + ":" + nodeID
}

// keyBelongsTo reports whether key is part of the named counter's state.
// The default counter owns every unprefixed replica key (each node's
// contribution) plus any "default:"-prefixed key; named counters own their
// "<name>:" namespace.
func (p *PNCounter) keyBelongsTo(name, key string) bool {
	if name == "" || name == "default" {
		return !strings.Contains(key, ":") || strings.HasPrefix(key, "default:")
	}
	return strings.HasPrefix(key, name+":")
}

// ValueName returns the value of a single named counter.
func (p *PNCounter) ValueName(name string) int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var pos, neg int64
	for k, v := range p.positive {
		if p.keyBelongsTo(name, k) {
			pos += v
		}
	}
	for k, v := range p.negative {
		if p.keyBelongsTo(name, k) {
			neg += v
		}
	}
	return pos - neg
}

// ResetName zeroes a single named counter, leaving every other counter
// untouched.
func (p *PNCounter) ResetName(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if name == "" || name == "default" {
		for k := range p.positive {
			if !strings.Contains(k, ":") || strings.HasPrefix(k, "default:") {
				delete(p.positive, k)
			}
		}
		for k := range p.negative {
			if !strings.Contains(k, ":") || strings.HasPrefix(k, "default:") {
				delete(p.negative, k)
			}
		}
		return
	}

	prefix := name + ":"
	for k := range p.positive {
		if strings.HasPrefix(k, prefix) {
			delete(p.positive, k)
		}
	}
	for k := range p.negative {
		if strings.HasPrefix(k, prefix) {
			delete(p.negative, k)
		}
	}
}

// Names returns the sorted list of named counters that currently have state,
// excluding the default counter.
func (p *PNCounter) Names() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	seen := make(map[string]bool)
	for k := range p.positive {
		if i := strings.IndexByte(k, ':'); i > 0 {
			seen[k[:i]] = true
		}
	}
	for k := range p.negative {
		if i := strings.IndexByte(k, ':'); i > 0 {
			seen[k[:i]] = true
		}
	}

	names := make([]string, 0, len(seen))
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Reset clears all per-replica counts back to zero.
func (p *PNCounter) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.positive = map[string]int64{}
	p.negative = map[string]int64{}
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
