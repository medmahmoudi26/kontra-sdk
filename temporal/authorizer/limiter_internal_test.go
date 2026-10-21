package authorizer

import (
	"fmt"
	"testing"
	"time"
)

// TestRelayBucketsAreSwept: full buckets (subjects idle for RelayBurst/RelayRate seconds)
// are dropped once the map reaches its sweep size, so what the limiter holds follows the
// relay traffic of the last few seconds, not every job ever relayed.
func TestRelayBucketsAreSwept(t *testing.T) {
	now := time.Unix(3_000_000, 0)
	l := &relayLimiter{now: func() time.Time { return now }, buckets: map[string]*relayBucket{}, sweepAt: relaySweepMin}
	for i := range 5 * relaySweepMin {
		if !l.allow(fmt.Sprintf("relay/acme/job-%032x", i)) {
			t.Fatal("a first request was refused")
		}
		now = now.Add(time.Second / RelayRate / 4) // each subject goes idle soon after
	}
	if len(l.buckets) > relaySweepMin+RelayBurst*RelayRate {
		t.Fatalf("%d buckets kept for %d subjects", len(l.buckets), 5*relaySweepMin)
	}
	// A busy subject's bucket survives a sweep: only full ones go.
	busy := "relay/acme/job-busy"
	for range RelayBurst {
		l.allow(busy)
	}
	l.sweep(now)
	if _, ok := l.buckets[busy]; !ok {
		t.Fatal("a spent bucket was swept")
	}
	if l.allow(busy) {
		t.Fatal("the swept map refilled a spent bucket")
	}
}
