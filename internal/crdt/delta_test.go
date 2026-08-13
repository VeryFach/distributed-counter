package crdt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeltaFromEmptyBaseReturnsEverything(t *testing.T) {
	positive := map[string]int64{"a": 10, "b": 5}
	negative := map[string]int64{"b": 2}
	clock := map[string]int64{"a": 3, "b": 1}

	deltaPos, deltaNeg, deltaClock := DeltaFrom(positive, negative, clock, nil)

	assert.Equal(t, positive, deltaPos)
	assert.Equal(t, negative, deltaNeg)
	assert.Equal(t, clock, deltaClock)
}

func TestDeltaFromSkipsEntriesPeerAlreadyHas(t *testing.T) {
	positive := map[string]int64{"a": 10, "b": 5}
	negative := map[string]int64{"b": 2}
	clock := map[string]int64{"a": 3, "b": 1}
	base := map[string]int64{"a": 3, "b": 1}

	deltaPos, deltaNeg, deltaClock := DeltaFrom(positive, negative, clock, base)

	assert.Empty(t, deltaPos, "no positive entries should be sent for synced nodes")
	assert.Empty(t, deltaNeg)
	assert.Empty(t, deltaClock)
}

func TestDeltaFromSendsOnlyChangedNodes(t *testing.T) {
	positive := map[string]int64{"a": 10, "b": 5}
	negative := map[string]int64{"b": 2}
	clock := map[string]int64{"a": 3, "b": 4}
	base := map[string]int64{"a": 3, "b": 1}

	deltaPos, deltaNeg, deltaClock := DeltaFrom(positive, negative, clock, base)

	assert.Equal(t, map[string]int64{"b": 5}, deltaPos)
	assert.Equal(t, map[string]int64{"b": 2}, deltaNeg)
	assert.Equal(t, map[string]int64{"b": 4}, deltaClock)
}

func TestMergeClockTakesMaxPerNode(t *testing.T) {
	a := map[string]int64{"x": 1, "y": 5}
	b := map[string]int64{"x": 3, "z": 2}

	merged := MergeClock(a, b)

	assert.Equal(t, int64(3), merged["x"])
	assert.Equal(t, int64(5), merged["y"])
	assert.Equal(t, int64(2), merged["z"])
}

func TestMaxClock(t *testing.T) {
	assert.Equal(t, int64(0), MaxClock(nil))
	assert.Equal(t, int64(7), MaxClock(map[string]int64{"a": 1, "b": 7, "c": 2}))
}

func TestClockNewerThan(t *testing.T) {
	assert.False(t, ClockNewerThan(map[string]int64{"a": 1}, map[string]int64{"a": 1}))
	assert.True(t, ClockNewerThan(map[string]int64{"a": 2}, map[string]int64{"a": 1}))
	assert.False(t, ClockNewerThan(nil, nil))
}

func TestClockNewerThanDetectsStaleBaseline(t *testing.T) {
	// A stale baseline is ahead of the current clock, e.g. after a local
	// Reset cleared the vector clock while the per-peer baseline kept its
	// old (higher) version. This must be detected so gossip falls back to
	// a full-state reconciliation.
	base := map[string]int64{"a": 5}
	resetClock := map[string]int64{}

	assert.True(t, ClockNewerThan(base, resetClock))

	// A healthy baseline never exceeds the current clock.
	current := map[string]int64{"a": 5, "b": 3}
	healthyBase := map[string]int64{"a": 5, "b": 3}
	assert.False(t, ClockNewerThan(healthyBase, current))
}

func TestDeltaFromEmptyWhenBaselineStale(t *testing.T) {
	positive := map[string]int64{"a": 7}
	clock := map[string]int64{"a": 1}
	// base is ahead of the clock -> nothing qualifies as a delta.
	base := map[string]int64{"a": 9}

	_, _, deltaClock := DeltaFrom(positive, nil, clock, base)

	assert.Empty(t, deltaClock)
}

func TestPNCounterReset(t *testing.T) {
	c := NewPNCounter("node-a")
	c.Increment(10)
	c.Decrement(3)
	assert.Equal(t, int64(7), c.Value())

	c.Reset()

	assert.Equal(t, int64(0), c.Value())
	assert.Empty(t, c.Positive())
	assert.Empty(t, c.Negative())
}

func TestVectorClockReset(t *testing.T) {
	v := NewVectorClock("node-a")
	v.Increment()
	v.Increment()
	v.MergeMap(map[string]int64{"node-b": 4})
	assert.Equal(t, int64(4), v.State()["node-b"])

	v.Reset()

	assert.Empty(t, v.State())
}

func TestPNCounterMergeWithDelta(t *testing.T) {
	local := NewPNCounter("node-a")
	local.Increment(10)

	// A delta update only carries the changed per-node entries.
	delta := NewPNCounter("")
	delta.SetPositive(map[string]int64{"node-b": 7})
	delta.SetNegative(map[string]int64{"node-b": 2})

	local.Merge(delta)

	assert.Equal(t, int64(10), local.Positive()["node-a"])
	assert.Equal(t, int64(7), local.Positive()["node-b"])
	assert.Equal(t, int64(2), local.Negative()["node-b"])
	assert.Equal(t, int64(15), local.Value())
}
