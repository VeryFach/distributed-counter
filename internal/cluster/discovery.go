package cluster

import (
	"sync"
)

type Discovery struct {
	mu        sync.RWMutex
	nodes     map[string]string
	seedNodes []string
}

func NewDiscovery(seedNodes []string) *Discovery {
	return &Discovery{
		nodes:     make(map[string]string),
		seedNodes: seedNodes,
	}
}

func (d *Discovery) GetNodeAddress(nodeID string) (string, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	addr, ok := d.nodes[nodeID]
	return addr, ok
}

func (d *Discovery) GetAllNodes() map[string]string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	copy := make(map[string]string)
	for k, v := range d.nodes {
		copy[k] = v
	}
	return copy
}

func (d *Discovery) AddNode(nodeID, address string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.nodes[nodeID] = address
}

func (d *Discovery) SeedNodes() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	res := make([]string, len(d.seedNodes))
	copy(res, d.seedNodes)

	return res
}