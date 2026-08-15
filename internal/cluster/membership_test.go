package cluster

import (
	"testing"
	"time"

	counter "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/stretchr/testify/assert"
)

func TestStatusTransitions(t *testing.T) {
	m := NewMembership("node-a")
	m.AddMember("node-b", "localhost:50052")

	member, _ := m.GetMember("node-b")
	assert.Equal(t, StatusAlive, member.Status)
	assert.True(t, member.IsActive)

	m.MarkSuspect("node-b")
	member, _ = m.GetMember("node-b")
	assert.Equal(t, StatusSuspect, member.Status)
	assert.False(t, member.IsActive)

	m.MarkDead("node-b")
	member, _ = m.GetMember("node-b")
	assert.Equal(t, StatusDead, member.Status)

	// A heartbeat revives the member.
	m.UpdateHeartbeat("node-b")
	member, _ = m.GetMember("node-b")
	assert.Equal(t, StatusAlive, member.Status)
	assert.True(t, member.IsActive)
}

func TestMarkStaleEscalatesSuspectToDead(t *testing.T) {
	m := NewMembership("node-a")
	m.AddOrUpdateMemberStatus("node-b", "localhost:50052", StatusAlive, time.Now().Add(-20*time.Second))

	changed := m.MarkStale(10 * time.Second)
	assert.Contains(t, changed, "node-b")

	member, _ := m.GetMember("node-b")
	assert.Equal(t, StatusSuspect, member.Status)

	changed = m.MarkStale(10 * time.Second)
	assert.Contains(t, changed, "node-b")

	member, _ = m.GetMember("node-b")
	assert.Equal(t, StatusDead, member.Status)
}

func TestGetRandomPeersOnlyActive(t *testing.T) {
	m := NewMembership("node-a")
	m.AddMember("node-b", "localhost:50052")
	m.AddMember("node-c", "localhost:50053")
	m.MarkDead("node-c")

	peers := m.GetRandomPeers(10)
	assert.Len(t, peers, 1)
	assert.Equal(t, "node-b", peers[0].ID)
}

func TestApplyMembershipGossip(t *testing.T) {
	m := NewMembership("node-a")
	m.AddMember("node-b", "localhost:50052")

	// A peer reports node-b as dead -> we did not verify it ourselves, so
	// it becomes Suspect rather than Dead immediately.
	m.ApplyMembership(map[string]counter.MemberStatus{
		"node-b": counter.MemberStatus_MEMBER_DEAD,
	})

	member, _ := m.GetMember("node-b")
	assert.Equal(t, StatusSuspect, member.Status)

	// Confirmed by a second source -> escalate to Dead.
	m.ApplyMembership(map[string]counter.MemberStatus{
		"node-b": counter.MemberStatus_MEMBER_DEAD,
	})

	member, _ = m.GetMember("node-b")
	assert.Equal(t, StatusDead, member.Status)
}

func TestStatusRoundTripProto(t *testing.T) {
	assert.Equal(t, counter.MemberStatus_MEMBER_DEAD, StatusDead.ToProto())
	assert.Equal(t, StatusSuspect, StatusFromProto(counter.MemberStatus_MEMBER_SUSPECT))
	assert.Equal(t, StatusAlive, StatusFromProto(counter.MemberStatus_MEMBER_ALIVE))
}