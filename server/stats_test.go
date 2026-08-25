package server

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/DrmagicE/gmqtt/persistence/subscription"
	"github.com/DrmagicE/gmqtt/pkg/packets"
)

type blockingSubscriptionStats struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (*blockingSubscriptionStats) GetStats() subscription.Stats { return subscription.Stats{} }

func (s *blockingSubscriptionStats) GetClientStats(string) (subscription.Stats, error) {
	s.calls.Add(1)
	close(s.entered)
	<-s.release
	return subscription.Stats{}, nil
}

func TestGetClientStatsDoesNotHoldClientLockWhileReadingSubscriptions(t *testing.T) {
	reader := &blockingSubscriptionStats{entered: make(chan struct{}), release: make(chan struct{})}
	manager := newStatsManager(reader)
	manager.clientStats["device"] = &ClientStats{}
	result := make(chan struct{})
	go func() {
		manager.GetClientStats("device")
		close(result)
	}()
	<-reader.entered
	updated := make(chan struct{})
	go func() {
		manager.packetReceived(&packets.Pingreq{}, "device")
		close(updated)
	}()
	select {
	case <-updated:
	case <-time.After(time.Second):
		t.Fatal("packet statistics blocked behind subscription statistics")
	}
	close(reader.release)
	<-result
	if reader.calls.Load() != 1 {
		t.Fatalf("subscription statistics calls = %d, want 1", reader.calls.Load())
	}
}
