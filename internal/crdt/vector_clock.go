package crdt

import (
    "encoding/json"
    "sync"
)

// VectorClock for tracking causality
type VectorClock struct {
    mu    sync.RWMutex
    clock map[string]int64
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
