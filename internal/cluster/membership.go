package cluster

import (
	"sync"
	"time"
)

type Member struct {
	ID            string
	Address       string
	IsActive      bool
	CounterValue  int64
	LastHeartbeat time.Time
}

type Membership struct {
	mu      sync.RWMutex
	nodeID  string
	members map[string]*Member
}

func NewMembership(nodeID string) *Membership {
	return &Membership{
		nodeID:  nodeID,
		members: make(map[string]*Member),
	}
}

func (m *Membership) AddMember(id, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.members[id]; !exists {
		m.members[id] = &Member{
			ID:            id,
			Address:       address,
			IsActive:      true,
			LastHeartbeat: time.Now(),
		}
	}
}

func (m *Membership) UpdateHeartbeat(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, exists := m.members[id]; exists {
		member.LastHeartbeat = time.Now()
		member.IsActive = true
	}
}

func (m *Membership) GetMembers() []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()

	members := make([]*Member, 0, len(m.members))
	for _, member := range m.members {
		members = append(members, member)
	}
	return members
}

func (m *Membership) GetRandomPeers(count int) []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Simple implementation - return first N active members
	peers := []*Member{}
	for _, member := range m.members {
		if member.ID != m.nodeID && member.IsActive {
			peers = append(peers, member)
			if len(peers) >= count {
				break
			}
		}
	}
	return peers
}

func (m *Membership) AddDiscoveredNodes(
	nodes map[string]string,
) {
	for id, addr := range nodes {
		if id == m.nodeID {
			continue
		}
		m.AddMember(id, addr)
	}
}
