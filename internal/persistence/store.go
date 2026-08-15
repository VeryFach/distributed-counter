package persistence

// CounterState is a snapshot of the CRDT state that must survive restarts:
// the per-replica positive/negative maps and the vector clock, plus the
// registered counter tags.
type CounterState struct {
	Positive map[string]int64 `json:"positive"`
	Negative map[string]int64 `json:"negative"`
	Clock    map[string]int64 `json:"clock"`
	Tags     map[string][]string `json:"tags,omitempty"`
}

// Store persists and restores counter state for a node.
type Store interface {
	// Save stores the given state, replacing any previous value.
	Save(nodeID string, state CounterState) error
	// Load returns the persisted state for nodeID, or nil when nothing
	// has been stored yet.
	Load(nodeID string) (*CounterState, error)
	// Close releases any underlying resources.
	Close() error
}
