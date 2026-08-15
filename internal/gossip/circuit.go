package gossip

import (
	"sync"
	"time"
)

// CircuitBreaker stops sending gossip to a peer that keeps failing, giving
// it a cooldown period before trying again. This prevents a dead node from
// being hammered by every gossip round.
type CircuitBreaker struct {
	mu       sync.Mutex
	failures int
	openUntil time.Time

	maxFailures int
	cooldown    time.Duration
}

func newCircuitBreaker(maxFailures int, cooldown time.Duration) *CircuitBreaker {
	if maxFailures <= 0 {
		maxFailures = 3
	}
	if cooldown <= 0 {
		cooldown = 30 * time.Second
	}
	return &CircuitBreaker{maxFailures: maxFailures, cooldown: cooldown}
}

// Allow reports whether a gossip attempt may be sent to this peer. When the
// circuit is open, the peer is skipped until the cooldown elapses.
func (c *CircuitBreaker) Allow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if time.Now().Before(c.openUntil) {
		return false
	}
	return true
}

// Success closes the circuit and resets the failure count.
func (c *CircuitBreaker) Success() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.failures = 0
	c.openUntil = time.Time{}
}

// Failure records a failed attempt. After maxFailures consecutive failures
// the circuit opens for the cooldown duration.
func (c *CircuitBreaker) Failure() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.failures++
	if c.failures >= c.maxFailures {
		c.openUntil = time.Now().Add(c.cooldown)
	}
}

func (c *CircuitBreaker) State() (failures int, open bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.failures, time.Now().Before(c.openUntil)
}