package crdt

// ClockNewerThan reports whether any entry in clock exceeds base.
func ClockNewerThan(clock, base map[string]int64) bool {
	for node, v := range clock {
		if v > base[node] {
			return true
		}
	}
	return false
}

// DeltaFrom returns only the per-node positive/negative/clock entries whose
// version exceeds base. This is safe for max-based CRDT merges: anything the
// peer already has is omitted, so delta gossip never loses or corrupts state.
func DeltaFrom(positive, negative, clock, base map[string]int64) (map[string]int64, map[string]int64, map[string]int64) {
	deltaPos := make(map[string]int64)
	deltaNeg := make(map[string]int64)
	deltaClock := make(map[string]int64)

	for node, v := range clock {
		if v > base[node] {
			if val, ok := positive[node]; ok {
				deltaPos[node] = val
			}
			if val, ok := negative[node]; ok {
				deltaNeg[node] = val
			}
			deltaClock[node] = v
		}
	}

	return deltaPos, deltaNeg, deltaClock
}

// MergeClock returns a new vector clock holding the max version per node.
func MergeClock(a, b map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(a)+len(b))

	for node, v := range a {
		out[node] = v
	}
	for node, v := range b {
		if v > out[node] {
			out[node] = v
		}
	}

	return out
}

// MaxClock returns the largest version value across the clock (0 when empty).
func MaxClock(clock map[string]int64) int64 {
	var max int64
	for _, v := range clock {
		if v > max {
			max = v
		}
	}
	return max
}
