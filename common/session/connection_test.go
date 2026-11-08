package session

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/task"
)

type connectionTestObservation struct {
	upload   atomic.Int64
	download atomic.Int64
	routes   atomic.Int64
	finishes atomic.Int64
	state    string
	finished chan struct{}
}

func (o *connectionTestObservation) Routed(ConnectionRoute)    { o.routes.Add(1) }
func (*connectionTestObservation) Destination(net.Destination) {}
func (o *connectionTestObservation) AddUpload(n int64)         { o.upload.Add(n) }
func (o *connectionTestObservation) AddDownload(n int64)       { o.download.Add(n) }
func (*connectionTestObservation) SetDownloadDeferred(bool)    {}
func (*connectionTestObservation) MarkBytesIncomplete()        {}
func (o *connectionTestObservation) Finish(state string) {
	o.state = state
	o.finishes.Add(1)
	if o.finished != nil {
		close(o.finished)
	}
}

func TestConnectionObservationRetainsMuxLifetime(t *testing.T) {
	o := new(connectionTestObservation)
	h := NewConnectionObservationHandle(o)
	releaseSession, releaseInput := h.Retain(), h.Retain()
	h.Finish("closed")
	h.Finish("closed")
	releaseSession("error")
	if o.finishes.Load() != 0 {
		t.Fatal("finished before mux input drained")
	}
	h.AddDownload(31)
	releaseInput("closed")
	releaseInput("closed")
	if o.finishes.Load() != 1 || o.state != "error" || o.download.Load() != 31 {
		t.Fatal("lost mux lifetime, outcome or bytes")
	}
}

func TestConnectionObservationTrafficDoesNotLockLifecycle(t *testing.T) {
	o := new(connectionTestObservation)
	h := NewConnectionObservationHandle(o)
	h.mu.Lock()
	done := make(chan struct{})
	go func() { h.AddUpload(17); h.AddDownload(23); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		h.mu.Unlock()
		t.Fatal("traffic waited for lifecycle mutex")
	}
	h.mu.Unlock()
	h.Finish("closed")
}

func TestConnectionObservationRouteAndFinishOnce(t *testing.T) {
	o := new(connectionTestObservation)
	h := NewConnectionObservationHandle(o)
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() { defer wg.Done(); h.Routed(ConnectionRoute{OutboundTag: "selected"}); h.Finish("closed") }()
	}
	wg.Wait()
	if o.routes.Load() != 1 || o.finishes.Load() != 1 {
		t.Fatal("duplicate route or completion")
	}
}

func TestConnectionObservationErrorAndInternalContext(t *testing.T) {
	o := new(connectionTestObservation)
	h := NewConnectionObservationHandle(o)
	ctx := ContextWithConnectionObservation(context.Background(), h)
	RecordConnectionError(ctx, errors.New("private failure that must not be forwarded"))
	SetConnectionOutcome(ctx, "interrupted")
	h.Finish("closed")
	if o.state != "error" {
		t.Fatalf("unknown failure became %s", o.state)
	}
	internal := ContextWithConnectionObservationDisabled(ctx)
	if !ConnectionObservationDisabledFromContext(internal) || ConnectionObservationFromContext(internal) != nil {
		t.Fatal("internal dial inherited application observation")
	}
}

func TestConnectionObservationCancellationBeforeMuxRelease(t *testing.T) {
	for _, initial := range []string{"closed", "blocked", "error"} {
		t.Run(initial, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			observation := new(connectionTestObservation)
			handle := NewConnectionObservationHandle(observation)
			handle.BindContext(ctx)
			release := handle.Retain()
			handle.SetOutcome(initial)
			handle.Finish("closed")
			cancel()
			release("closed")
			want := initial
			if initial == "closed" {
				want = "interrupted"
			}
			if observation.finishes.Load() != 1 || observation.state != want {
				t.Fatalf("canceled %s flow became %s", initial, observation.state)
			}
		})
	}
}

func TestConnectionObservationTaskRunRetainsUnfinishedDownload(t *testing.T) {
	observation := &connectionTestObservation{finished: make(chan struct{})}
	handle := NewConnectionObservationHandle(observation)
	ctx := ContextWithConnectionObservation(context.Background(), handle)
	download := make(chan struct{})
	defer close(download)
	err := task.Run(ctx, func() error { return errors.New("upload failed") }, func() error {
		<-download
		handle.AddDownload(17)
		return nil
	})
	if err == nil {
		t.Fatal("expected early upload error")
	}
	RecordConnectionError(ctx, err)
	handle.Finish("closed")
	if observation.finishes.Load() != 0 {
		t.Fatal("unfinished task was not retained before launch")
	}
	download <- struct{}{}
	select {
	case <-observation.finished:
	case <-time.After(time.Second):
		t.Fatal("download task did not release observation")
	}
	if observation.download.Load() != 17 || observation.state != "error" {
		t.Fatal("lost late download bytes or upload failure")
	}
}

func TestConnectionObservationDisabledTasksDoNotRetain(t *testing.T) {
	observation := &connectionTestObservation{}
	handle := NewConnectionObservationHandle(observation)
	ctx := ContextWithConnectionObservation(context.Background(), handle)
	ctx = ContextWithConnectionObservationDisabled(ctx)
	download, exited := make(chan struct{}), make(chan struct{})
	err := task.Run(ctx, func() error { return errors.New("expected error") }, func() error {
		defer close(exited)
		<-download
		return nil
	})
	if err == nil {
		t.Fatal("expected early return")
	}
	handle.Finish("closed")
	close(download)
	<-exited
	if observation.finishes.Load() != 1 {
		t.Fatal("disabled task inherited application lifetime")
	}
}

func TestConnectionObservationDetachedContextKeepsOnlyAnalytics(t *testing.T) {
	type unrelatedKey struct{}
	original, cancel := context.WithCancel(context.WithValue(context.Background(), unrelatedKey{}, true))
	defer cancel()
	handle := NewConnectionObservationHandle(new(connectionTestObservation))
	ctx := ContextWithConnectionObservation(original, handle)
	detached := ConnectionObservationDetachedContext(ctx)
	cancel()
	if detached.Err() != nil || detached.Value(unrelatedKey{}) != nil || ConnectionObservationFromContext(detached) != handle {
		t.Fatal("detached analytics context changed timeout-only semantics")
	}
	internal := ConnectionObservationDetachedContext(ContextWithConnectionObservationDisabled(ctx))
	if !ConnectionObservationDisabledFromContext(internal) || ConnectionObservationFromContext(internal) != nil {
		t.Fatal("detached internal dial became observable")
	}
	handle.Finish("closed")
}
