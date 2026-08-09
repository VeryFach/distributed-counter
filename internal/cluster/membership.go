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
	m.AddOrUpdateMember(id, address, true, time.Now())
}

func (m *Membership) AddOrUpdateMember(id, address string, isActive bool, lastHeartbeat time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, exists := m.members[id]; exists {
		member.Address = address
		member.IsActive = isActive
		member.LastHeartbeat = lastHeartbeat
		return
	}

	m.members[id] = &Member{
		ID:            id,
		Address:       address,
		IsActive:      isActive,
		LastHeartbeat: lastHeartbeat,
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

func (m *Membership) MarkInactive(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, exists := m.members[id]; exists {
		member.IsActive = false
	}
}

func (m *Membership) MarkStale(threshold time.Duration) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	stale := make([]string, 0)

	for id, member := range m.members {
		if id == m.nodeID {
			continue
		}

		if member.IsActive && now.Sub(member.LastHeartbeat) > threshold {
			member.IsActive = false
			stale = append(stale, id)
		}
	}

	return stale
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

func (m *Membership) GetMember(id string) (*Member, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	member, exists := m.members[id]
	return member, exists
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
