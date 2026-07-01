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