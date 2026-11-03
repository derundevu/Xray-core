package observatory

import (
	"context"
	"sync"
	"testing"
)

func TestObserverUpdateStatusPrunesStaleOutbounds(t *testing.T) {
	observer := &Observer{
		status: []*OutboundStatus{
			{
				OutboundTag:     "keep",
				Alive:           true,
				Delay:           42,
				LastErrorReason: "",
				LastSeenTime:    111,
				LastTryTime:     222,
			},
			{
				OutboundTag:     "drop",
				Alive:           false,
				Delay:           99999999,
				LastErrorReason: "probe failed",
				LastSeenTime:    333,
				LastTryTime:     444,
			},
		},
	}

	observer.clearRemovedOutbounds([]string{"keep"})

	if len(observer.status) != 1 {
		t.Fatalf("expected 1 status after pruning, got %d", len(observer.status))
	}

	got := observer.status[0]
	if got.OutboundTag != "keep" {
		t.Fatalf("expected remaining status for keep, got %q", got.OutboundTag)
	}
	if !got.Alive {
		t.Fatal("expected remaining status to preserve Alive field")
	}
	if got.Delay != 42 {
		t.Fatalf("expected remaining status to preserve Delay, got %d", got.Delay)
	}
	if got.LastSeenTime != 111 {
		t.Fatalf("expected remaining status to preserve LastSeenTime, got %d", got.LastSeenTime)
	}
	if got.LastTryTime != 222 {
		t.Fatalf("expected remaining status to preserve LastTryTime, got %d", got.LastTryTime)
	}
}

func TestObserverUpdateStatusClearsWhenNoOutboundsRemain(t *testing.T) {
	observer := &Observer{
		status: []*OutboundStatus{
			{OutboundTag: "drop-1"},
			{OutboundTag: "drop-2"},
		},
	}

	observer.clearRemovedOutbounds(nil)

	if len(observer.status) != 0 {
		t.Fatalf("expected all statuses to be removed, got %d", len(observer.status))
	}
}

func TestObserverGetObservationReturnsDetachedSnapshot(t *testing.T) {
	observer := &Observer{status: []*OutboundStatus{{
		OutboundTag: "member",
		Alive:       true,
		Delay:       42,
		HealthPing:  &HealthPingMeasurementResult{All: 5},
	}}}
	message, err := observer.GetObservation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	snapshot := message.(*ObservationResult)
	snapshot.Status[0].HealthPing.All = 99
	if observer.status[0].HealthPing.All != 5 {
		t.Fatal("observation aliases nested status data")
	}
	observer.updateStatusForResult("member", &ProbeResult{Alive: false, LastErrorReason: "offline"})
	observer.clearRemovedOutbounds(nil)
	if len(snapshot.Status) != 1 || !snapshot.Status[0].Alive || snapshot.Status[0].Delay != 42 {
		t.Fatal("later status update or removal changed the returned snapshot")
	}
}

func TestObserverGetObservationConcurrentUpdates(t *testing.T) {
	observer := &Observer{}
	var updates sync.WaitGroup
	updates.Add(1)
	go func() {
		defer updates.Done()
		for i := range 100 {
			observer.updateStatusForResult("member", &ProbeResult{Alive: true, Delay: int64(i)})
			observer.clearRemovedOutbounds(nil)
		}
	}()
	for range 100 {
		message, err := observer.GetObservation(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, status := range message.(*ObservationResult).Status {
			if status.OutboundTag != "member" || !status.Alive {
				t.Errorf("inconsistent status snapshot: %v", status)
			}
		}
	}
	updates.Wait()
}
