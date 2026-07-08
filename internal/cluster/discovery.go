package cluster

import (
	"sync"
)

type Discovery struct {
	mu			sync.RWMutex
	nodes		map[string]string
	seeNodes 	[]string
}

func newDiscovery(seeNodes []string) *Discovery{
	return  &Discovery{
		nodes: make(map[string]string),
		seeNodes: seeNodes,
	}
}

func (d *Discovery) getNodeAddress(nodeID, address string) (string, bool){
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