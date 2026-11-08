package session

import (
	"context"
	goerrors "errors"
	"io"
	"sync"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/task"
)

// ConnectionMetadata is an application flow snapshot, never a live session pointer.
type ConnectionMetadata struct {
	Network     net.Network
	Source      net.Destination
	Destination net.Destination
	InboundTag  string
	User        string
}

type ConnectionRoute struct {
	OutboundTag string
	Target      net.Destination
	Domain      string
}

// ConnectionObservation receives metadata and byte counts only, never payload or errors.
// Implementations must support concurrent calls and must not block forwarding.
type ConnectionObservation interface {
	Routed(ConnectionRoute)
	Destination(net.Destination)
	AddUpload(int64)
	AddDownload(int64)
	SetDownloadDeferred(bool)
	MarkBytesIncomplete()
	Finish(string)
}

type connectionContextKey uint8

const (
	connectionObservationKey connectionContextKey = iota
	connectionObservationDisabledKey
)

func ContextWithConnectionObservation(ctx context.Context, observation ConnectionObservation) context.Context {
	lifetime, _ := observation.(task.Lifetime)
	ctx = task.ContextWithLifetime(ctx, lifetime)
	return context.WithValue(ctx, connectionObservationKey, observation)
}

// ConnectionObservationDetachedContext preserves only analytics ownership when
// a TimeoutOnly outbound deliberately discards cancellation and session values.
func ConnectionObservationDetachedContext(ctx context.Context) context.Context {
	if ConnectionObservationDisabledFromContext(ctx) {
		return ContextWithConnectionObservationDisabled(context.Background())
	}
	return ContextWithConnectionObservation(context.Background(), ConnectionObservationFromContext(ctx))
}

func ConnectionObservationFromContext(ctx context.Context) ConnectionObservation {
	observation, _ := ctx.Value(connectionObservationKey).(ConnectionObservation)
	return observation
}

func ContextWithConnectionObservationDisabled(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, connectionObservationDisabledKey, true)
	return ContextWithConnectionObservation(ctx, nil)
}

func ConnectionObservationDisabledFromContext(ctx context.Context) bool {
	disabled, _ := ctx.Value(connectionObservationDisabledKey).(bool)
	return disabled
}

// ConnectionObservationHandle owns a logical flow's lifecycle. Retained async work
// (notably mux sessions) must drain before Finish reaches the observer. The mutex
// protects lifecycle only; byte accounting never takes it.
type ConnectionObservationHandle struct {
	ConnectionObservation
	mu         sync.Mutex
	routeOnce  sync.Once
	refs       int
	outcome    string
	finished   bool
	stop       func() bool
	ctx        context.Context
	finishOnce sync.Once
}

func NewConnectionObservationHandle(observation ConnectionObservation) *ConnectionObservationHandle {
	return &ConnectionObservationHandle{ConnectionObservation: observation, refs: 1, outcome: "closed"}
}

func (h *ConnectionObservationHandle) Routed(route ConnectionRoute) {
	h.routeOnce.Do(func() { h.ConnectionObservation.Routed(route) })
}

func (h *ConnectionObservationHandle) SetOutcome(state string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	// Preserve a specific rejection or failure across cleanup cancellation.
	if !h.finished && outcomePriority(state) > outcomePriority(h.outcome) {
		h.outcome = state
	}
}

func outcomePriority(state string) int {
	switch state {
	case "blocked":
		return 3
	case "error":
		return 2
	case "interrupted":
		return 1
	default:
		return 0
	}
}

func (h *ConnectionObservationHandle) Retain() func(string) {
	h.mu.Lock()
	if h.finished {
		h.mu.Unlock()
		return nil
	}
	h.refs++
	h.mu.Unlock()
	var once sync.Once
	return func(state string) { once.Do(func() { h.release(state) }) }
}

// RetainTask is installed on task.Run contexts before task goroutines launch.
// It adds no buffer-path work and never waits for or cancels forwarding.
func (h *ConnectionObservationHandle) RetainTask() func(error) {
	h.mu.Lock()
	if h.finished {
		h.mu.Unlock()
		return nil
	}
	h.refs++
	ctx := h.ctx
	h.mu.Unlock()
	var once sync.Once
	return func(err error) {
		once.Do(func() { h.release(connectionErrorOutcome(ctx, err)) })
	}
}

func (h *ConnectionObservationHandle) release(state string) {
	h.mu.Lock()
	if h.finished {
		h.mu.Unlock()
		return
	}
	if outcomePriority(state) > outcomePriority(h.outcome) {
		h.outcome = state
	}
	h.refs--
	if h.refs != 0 {
		h.mu.Unlock()
		return
	}
	// Do not depend on AfterFunc scheduling: a mux may finish concurrently
	// with cancellation, before its cancellation callback gets CPU time.
	if h.ctx != nil && h.ctx.Err() != nil && outcomePriority("interrupted") > outcomePriority(h.outcome) {
		h.outcome = "interrupted"
	}
	h.finished = true
	state = h.outcome
	stop := h.stop
	h.stop = nil
	h.ctx = nil
	h.mu.Unlock()
	if stop != nil {
		stop()
	}
	h.ConnectionObservation.Finish(state)
}

// Finish releases the dispatch owner exactly once. Retain callbacks release only
// their own ownership, so repeated cleanup cannot close an active mux session.
func (h *ConnectionObservationHandle) Finish(state string) {
	h.finishOnce.Do(func() { h.release(state) })
}

func (h *ConnectionObservationHandle) BindContext(ctx context.Context) {
	stop := context.AfterFunc(ctx, func() { h.SetOutcome("interrupted") })
	h.mu.Lock()
	finished := h.finished
	if !finished {
		h.stop, h.ctx = stop, ctx
	}
	h.mu.Unlock()
	if finished {
		stop()
	}
}

func SetConnectionOutcome(ctx context.Context, state string) {
	if h, ok := ConnectionObservationFromContext(ctx).(*ConnectionObservationHandle); ok {
		h.SetOutcome(state)
	}
}

func RetainConnectionObservation(ctx context.Context) func(string) {
	if h, ok := ConnectionObservationFromContext(ctx).(*ConnectionObservationHandle); ok {
		return h.Retain()
	}
	return nil
}

// RecordConnectionError classifies locally; raw errors never reach observers.
func RecordConnectionError(ctx context.Context, err error) {
	SetConnectionOutcome(ctx, connectionErrorOutcome(ctx, err))
}

func connectionErrorOutcome(ctx context.Context, err error) string {
	if err == nil {
		return "closed"
	}
	cause := errors.Cause(err)
	if goerrors.Is(cause, io.EOF) {
		return "closed"
	}
	if (ctx != nil && ctx.Err() != nil) || goerrors.Is(cause, context.Canceled) || goerrors.Is(cause, context.DeadlineExceeded) || goerrors.Is(cause, io.ErrClosedPipe) {
		return "interrupted"
	}
	return "error"
}
