package crdt

import (
	"testing"
)

func TestPNCounterNamespacedCounters(t *testing.T) {
	p := NewPNCounter("node-a")

	p.IncrementName("post_1", 5)
	p.IncrementName("post_2", 3)
	p.IncrementName("", 2) // default
	p.DecrementName("post_2", 1)

	if got := p.ValueName("post_1"); got != 5 {
		t.Fatalf("post_1 = %d, want 5", got)
	}
	if got := p.ValueName("post_2"); got != 2 {
		t.Fatalf("post_2 = %d, want 2", got)
	}
	if got := p.ValueName("default"); got != 2 {
		t.Fatalf("default = %d, want 2", got)
	}
	if got := p.ValueName("nonexistent"); got != 0 {
		t.Fatalf("nonexistent = %d, want 0", got)
	}

	// Total across all counters.
	if got := p.Value(); got != 9 {
		t.Fatalf("total = %d, want 9", got)
	}

	// Names only reports named counters, not the default one.
	names := map[string]bool{}
	for _, n := range p.Names() {
		names[n] = true
	}
	if !names["post_1"] || !names["post_2"] || names["default"] || len(names) != 2 {
		t.Fatalf("unexpected names: %v", names)
	}
}

func TestPNCounterMergeAcrossReplicasKeepsCountersSeparate(t *testing.T) {
	a := NewPNCounter("node-a")
	b := NewPNCounter("node-b")

	a.IncrementName("post_1", 10)
	b.IncrementName("post_1", 5)
	b.IncrementName("post_2", 7)
	a.IncrementName("", 1)

	a.Merge(b)
	b.Merge(a)

	if got := a.ValueName("post_1"); got != 15 {
		t.Fatalf("post_1 after merge = %d, want 15", got)
	}
	if got := a.ValueName("post_2"); got != 7 {
		t.Fatalf("post_2 after merge = %d, want 7", got)
	}
	if got := a.ValueName("default"); got != 1 {
		t.Fatalf("default after merge = %d, want 1", got)
	}
}

func TestPNCounterResetNameScoped(t *testing.T) {
	p := NewPNCounter("node-a")

	p.IncrementName("post_1", 5)
	p.IncrementName("post_2", 3)
	p.IncrementName("", 2)

	p.ResetName("post_1")

	if got := p.ValueName("post_1"); got != 0 {
		t.Fatalf("post_1 after reset = %d, want 0", got)
	}
	if got := p.ValueName("post_2"); got != 3 {
		t.Fatalf("post_2 after reset = %d, want 3", got)
	}
	if got := p.ValueName("default"); got != 2 {
		t.Fatalf("default after reset = %d, want 2", got)
	}
}

func TestVectorClockNamespaced(t *testing.T) {
	v := NewVectorClock("node-a")

	v.IncrementName("post_1")
	v.IncrementName("post_1")
	v.IncrementName("post_2")
	v.Increment()

	if v.clock["post_1:node-a"] != 2 {
		t.Fatalf("post_1 clock = %d, want 2", v.clock["post_1:node-a"])
	}
	if v.clock["post_2:node-a"] != 1 {
		t.Fatalf("post_2 clock = %d, want 1", v.clock["post_2:node-a"])
	}
	if v.clock["node-a"] != 1 {
		t.Fatalf("default clock = %d, want 1", v.clock["node-a"])
	}

	v.ResetName("post_1")
	if _, ok := v.clock["post_1:node-a"]; ok {
		t.Fatal("post_1 clock entry survived reset")
	}
	if v.clock["post_2:node-a"] != 1 {
		t.Fatalf("post_2 clock changed after post_1 reset: %d", v.clock["post_2:node-a"])
	}
}
