package crdt

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"
)

// VectorClock for tracking causality
type VectorClock struct {
	mu     sync.RWMutex
	clock  map[string]int64
	nodeID string
}

func NewVectorClock(nodeID string) *VectorClock {
	return &VectorClock{
		clock:  make(map[string]int64),
		nodeID: nodeID,
	}
}

func (v *VectorClock) MergeMap(state map[string]int64) {
	v.mu.Lock()
	defer v.mu.Unlock()

	for node, version := range state {
		if v.clock[node] < version {
			v.clock[node] = version
		}
	}
}

func (v *VectorClock) Increment() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.clock[v.nodeID]++
}

// IncrementName ticks the clock for a named counter. Keys use the same
// naming scheme as PNCounter so causal order is tracked per counter.
func (v *VectorClock) IncrementName(name string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.clock[counterKey(name, v.nodeID)]++
}

// ResetName clears the clock entries belonging to a single named counter.
func (v *VectorClock) ResetName(name string) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if name == "" || name == "default" {
		for k := range v.clock {
			if !strings.Contains(k, ":") || strings.HasPrefix(k, "default:") {
				delete(v.clock, k)
			}
		}
		return
	}

	prefix := name + ":"
	for k := range v.clock {
		if strings.HasPrefix(k, prefix) {
			delete(v.clock, k)
		}
	}
}

// Names returns the sorted list of named counters tracked by the clock,
// excluding the default counter.
func (v *VectorClock) Names() []string {
	v.mu.RLock()
	defer v.mu.RUnlock()

	seen := make(map[string]bool)
	for k := range v.clock {
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

// Reset clears all clock entries.
func (v *VectorClock) Reset() {
	v.mu.Lock()
	defer v.mu.Unlock()

	v.clock = map[string]int64{}
}

func (v *VectorClock) Merge(other *VectorClock) {
	v.mu.Lock()
	defer v.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	for nodeID, version := range other.clock {
		if v.clock[nodeID] < version {
			v.clock[nodeID] = version
		}
	}
}

func (v *VectorClock) String() string {
	v.mu.RLock()
	defer v.mu.RUnlock()

	b, _ := json.Marshal(v.clock)
	return string(b)
}

func (v *VectorClock) State() map[string]int64 {
	v.mu.RLock()
	defer v.mu.RUnlock()

	res := make(map[string]int64)
	for k, val := range v.clock {
		res[k] = val
	}
	return res
}
