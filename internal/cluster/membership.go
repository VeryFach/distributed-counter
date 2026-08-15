package cluster

import (
	"math/rand"
	"sync"
	"time"

	counter "github.com/VeryFach/distributed-counter/api/proto"
)

// Status is the lifecycle state of a cluster member, modelled after
// gossip membership protocols (Cassandra/ScyllaDB/Serf).
type Status int

const (
	// StatusAlive is the default healthy state.
	StatusAlive Status = iota
	// StatusSuspect means the node may be down (SWIM failure detection).
	StatusSuspect
	// StatusDead means the node is confirmed unreachable.
	StatusDead
	// StatusLeft means the node left the cluster deliberately.
	StatusLeft
)

func (s Status) String() string {
	switch s {
	case StatusAlive:
		return "Alive"
	case StatusSuspect:
		return "Suspect"
	case StatusDead:
		return "Dead"
	case StatusLeft:
		return "Left"
	default:
		return "Unknown"
	}
}

// ToProto converts a status to its protobuf representation.
func (s Status) ToProto() counter.MemberStatus {
	switch s {
	case StatusSuspect:
		return counter.MemberStatus_MEMBER_SUSPECT
	case StatusDead:
		return counter.MemberStatus_MEMBER_DEAD
	case StatusLeft:
		return counter.MemberStatus_MEMBER_LEFT
	default:
		return counter.MemberStatus_MEMBER_ALIVE
	}
}

// StatusFromProto converts a protobuf status back to the local type.
func StatusFromProto(s counter.MemberStatus) Status {
	switch s {
	case counter.MemberStatus_MEMBER_SUSPECT:
		return StatusSuspect
	case counter.MemberStatus_MEMBER_DEAD:
		return StatusDead
	case counter.MemberStatus_MEMBER_LEFT:
		return StatusLeft
	default:
		return StatusAlive
	}
}

type Member struct {
	ID            string
	Address       string
	Status        Status
	IsActive      bool
	CounterValue  int64
	LastHeartbeat time.Time
	// SuspectCount tracks consecutive failed probes while in Suspect,
	// letting the failure detector escalate Suspect -> Dead.
	SuspectCount int
	// Priority is the Bully election priority, learned from heartbeats and
	// the join protocol.
	Priority int64
}

func (m *Member) setStatus(s Status) {
	m.Status = s
	m.IsActive = s == StatusAlive
}

type Membership struct {
	mu         sync.RWMutex
	nodeID     string
	members    map[string]*Member
	recovering bool
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
	status := StatusDead
	if isActive {
		status = StatusAlive
	}
	m.AddOrUpdateMemberStatus(id, address, status, lastHeartbeat)
}

func (m *Membership) AddOrUpdateMemberStatus(id, address string, status Status, lastHeartbeat time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, exists := m.members[id]; exists {
		member.Address = address
		member.LastHeartbeat = lastHeartbeat
		// A node that comes back reports itself as alive; accept it. Other
		// transitions (Suspect/Dead) are only applied via the detector.
		if status == StatusAlive {
			member.setStatus(StatusAlive)
			member.SuspectCount = 0
		}
		return
	}

	member := &Member{
		ID:            id,
		Address:       address,
		LastHeartbeat: lastHeartbeat,
	}
	member.setStatus(status)
	m.members[id] = member
}

func (m *Membership) UpdateHeartbeat(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, exists := m.members[id]; exists {
		member.LastHeartbeat = time.Now()
		member.setStatus(StatusAlive)
		member.SuspectCount = 0
	}
}

// SetPriority records the Bully election priority of a member. Priorities
// are learned from the heartbeat/join protocols and drive leader election.
func (m *Membership) SetPriority(id string, priority int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, exists := m.members[id]; exists {
		member.Priority = priority
	}
}

func (m *Membership) MarkSuspect(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, exists := m.members[id]; exists {
		// Dead/Left are terminal: a failed heartbeat must not demote a
		// confirmed-dead node back to Suspect (that would oscillate the
		// status every heartbeat cycle). The node only returns to the
		// cluster when it reports itself alive again.
		if member.Status == StatusDead || member.Status == StatusLeft {
			return
		}
		member.setStatus(StatusSuspect)
		member.SuspectCount++
	}
}

func (m *Membership) MarkDead(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, exists := m.members[id]; exists {
		member.setStatus(StatusDead)
	}
}

func (m *Membership) MarkLeft(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if member, exists := m.members[id]; exists {
		member.setStatus(StatusLeft)
	}
}

// MarkInactive is kept for the heartbeat path: a failed heartbeat means the
// node is suspect, not dead.
func (m *Membership) MarkInactive(id string) {
	m.MarkSuspect(id)
}

// MarkStale escalates members whose heartbeat is older than threshold.
// Alive -> Suspect on the first miss, Suspect -> Dead on a sustained miss.
// Returns the ids whose status changed.
func (m *Membership) MarkStale(threshold time.Duration) []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	changed := make([]string, 0)

	for id, member := range m.members {
		if id == m.nodeID {
			continue
		}

		stale := now.Sub(member.LastHeartbeat) > threshold
		if !stale {
			continue
		}

		switch member.Status {
		case StatusAlive:
			member.setStatus(StatusSuspect)
			member.SuspectCount++
			changed = append(changed, id)
		case StatusSuspect:
			member.setStatus(StatusDead)
			changed = append(changed, id)
		}
	}

	return changed
}

// SetRecovering sets the recovery flag for observability.
func (m *Membership) SetRecovering(recovering bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.recovering = recovering
}

func (m *Membership) IsRecovering() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.recovering
}

func (m *Membership) GetMembers() []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()

	members := make([]*Member, 0, len(m.members))
	for _, member := range m.members {
		copy := *member
		members = append(members, &copy)
	}
	return members
}

func (m *Membership) GetMember(id string) (*Member, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	member, exists := m.members[id]
	if !exists {
		return nil, false
	}
	copy := *member
	return &copy, true
}

// GetRandomPeers returns up to count active peers, shuffled so gossip
// spreads evenly instead of always hitting the first N members.
func (m *Membership) GetRandomPeers(count int) []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := []*Member{}
	for _, member := range m.members {
		if member.ID != m.nodeID && member.Status == StatusAlive {
			copy := *member
			peers = append(peers, &copy)
		}
	}

	rand.Shuffle(len(peers), func(i, j int) {
		peers[i], peers[j] = peers[j], peers[i]
	})

	if len(peers) > count {
		peers = peers[:count]
	}

	return peers
}

// GetRandomPeer returns one active peer, or nil when none exist.
func (m *Membership) GetRandomPeer() *Member {
	peers := m.GetRandomPeers(1)
	if len(peers) == 0 {
		return nil
	}
	return peers[0]
}

// GetRandomProbeTarget returns a random member for the failure detector to
// probe. Suspect members are preferred so they are actively re-verified and
// can be escalated to Dead; alive members are probed as normal SWIM targets.
func (m *Membership) GetRandomProbeTarget() *Member {
	m.mu.RLock()
	defer m.mu.RUnlock()

	candidates := []*Member{}
	for _, member := range m.members {
		if member.ID == m.nodeID {
			continue
		}
		if member.Status == StatusAlive || member.Status == StatusSuspect {
			copy := *member
			candidates = append(candidates, &copy)
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})

	return candidates[0]
}

// GetAlivePeers returns all active peers regardless of count.
func (m *Membership) GetAlivePeers() []*Member {
	m.mu.RLock()
	defer m.mu.RUnlock()

	peers := []*Member{}
	for _, member := range m.members {
		if member.ID != m.nodeID && member.Status == StatusAlive {
			copy := *member
			peers = append(peers, &copy)
		}
	}
	return peers
}

func (m *Membership) AddDiscoveredNodes(nodes map[string]string) {
	for id, addr := range nodes {
		if id == m.nodeID {
			continue
		}
		m.AddMember(id, addr)
	}
}

// GossipMembership returns the current status map to be piggybacked on
// gossip state updates, so Suspect/Dead transitions propagate cluster-wide.
func (m *Membership) GossipMembership() map[string]counter.MemberStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string]counter.MemberStatus)
	for id, member := range m.members {
		if id == m.nodeID {
			continue
		}
		out[id] = member.Status.ToProto()
	}
	return out
}

// ApplyMembership merges statuses learned from a peer's gossip. A node that
// another member reports as Dead becomes Suspect locally (we did not verify
// it ourselves); a confirmed Suspect from two sources escalates to Dead.
func (m *Membership) ApplyMembership(statuses map[string]counter.MemberStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, status := range statuses {
		if id == m.nodeID {
			continue
		}

		member, exists := m.members[id]
		if !exists {
			continue
		}

		switch StatusFromProto(status) {
		case StatusDead:
			if member.Status == StatusAlive {
				member.setStatus(StatusSuspect)
				member.SuspectCount++
			} else if member.SuspectCount >= 1 {
				member.setStatus(StatusDead)
			}
		case StatusSuspect:
			if member.Status == StatusAlive {
				member.setStatus(StatusSuspect)
				member.SuspectCount++
			}
		case StatusLeft:
			member.setStatus(StatusLeft)
		}
	}
}