package gossip

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCircuitBreakerOpensAfterFailures(t *testing.T) {
	cb := newCircuitBreaker(3, time.Minute)

	for i := 0; i < 3; i++ {
		cb.Failure()
	}

	assert.False(t, cb.Allow(), "circuit should be open after max failures")
}

func TestCircuitBreakerResetsOnSuccess(t *testing.T) {
	cb := newCircuitBreaker(3, time.Minute)

	cb.Failure()
	cb.Failure()
	cb.Success()

	assert.True(t, cb.Allow(), "circuit should be closed after success")
}

func TestCircuitBreakerReopensAfterCooldown(t *testing.T) {
	cb := newCircuitBreaker(3, 50*time.Millisecond)

	for i := 0; i < 3; i++ {
		cb.Failure()
	}
	assert.False(t, cb.Allow())

	time.Sleep(60 * time.Millisecond)
	assert.True(t, cb.Allow(), "circuit should allow after cooldown elapses")
}