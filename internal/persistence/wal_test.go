package persistence

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALAppendReplayTruncate(t *testing.T) {
	dir := t.TempDir()
	store, err := NewWALStore(dir)
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.Append("node-a", "increment", 5, nil, nil, nil))
	require.NoError(t, store.Append("node-a", "decrement", 2, nil, nil, nil))

	entries, err := store.Replay("node-a")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, "increment", entries[0].Op)
	assert.Equal(t, int64(5), entries[0].Delta)
	assert.Equal(t, "decrement", entries[1].Op)
	assert.Equal(t, int64(2), entries[1].Delta)
	assert.True(t, entries[1].Seq > entries[0].Seq)

	require.NoError(t, store.Truncate("node-a"))

	entries, err = store.Replay("node-a")
	require.NoError(t, err)
	assert.Len(t, entries, 0)
}

func TestWALSeqContinuesAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	store, err := NewWALStore(dir)
	require.NoError(t, err)
	require.NoError(t, store.Append("node-a", "increment", 1, nil, nil, nil))
	require.NoError(t, store.Close())

	// Simulate restart: a new store reads the same log and continues
	// the sequence numbers.
	store2, err := NewWALStore(dir)
	require.NoError(t, err)
	defer store2.Close()

	entries, err := store2.Replay("node-a")
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, uint64(1), entries[0].Seq)

	require.NoError(t, store2.Append("node-a", "increment", 2, nil, nil, nil))

	entries, err = store2.Replay("node-a")
	require.NoError(t, err)
	require.Len(t, entries, 2)
	assert.Equal(t, uint64(2), entries[1].Seq)
}

func TestWALReplayMissingFileReturnsNil(t *testing.T) {
	dir := t.TempDir()
	store, err := NewWALStore(dir)
	require.NoError(t, err)
	defer store.Close()

	entries, err := store.Replay("no-such-node")
	require.NoError(t, err)
	assert.Nil(t, entries)
}