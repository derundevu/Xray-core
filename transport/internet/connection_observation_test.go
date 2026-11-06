package internet

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
)

type chainedObservation struct {
	finishes atomic.Int64
	done     chan struct{}
}

func (*chainedObservation) Routed(session.ConnectionRoute) {}
func (*chainedObservation) Destination(net.Destination)    {}
func (*chainedObservation) AddUpload(int64)                {}
func (*chainedObservation) AddDownload(int64)              {}
func (*chainedObservation) SetDownloadDeferred(bool)       {}
func (*chainedObservation) MarkBytesIncomplete()           {}
func (o *chainedObservation) Finish(string)                { o.finishes.Add(1); close(o.done) }

type delayedChainedHandler struct{ release <-chan struct{} }

func (*delayedChainedHandler) Start() error                                { return nil }
func (*delayedChainedHandler) Close() error                                { return nil }
func (*delayedChainedHandler) Tag() string                                 { return "chain" }
func (*delayedChainedHandler) SenderSettings() *serial.TypedMessage        { return nil }
func (*delayedChainedHandler) ProxySettings() *serial.TypedMessage         { return nil }
func (h *delayedChainedHandler) Dispatch(context.Context, *transport.Link) { <-h.release }

func TestChainedDispatchRetainsObservationBeforeLaunch(t *testing.T) {
	observation := &chainedObservation{done: make(chan struct{})}
	handle := session.NewConnectionObservationHandle(observation)
	ctx := session.ContextWithConnectionObservation(context.Background(), handle)
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	release := make(chan struct{})
	conn := redirect(ctx, net.TCPDestination(net.LocalHostIP, 80), "chain", &delayedChainedHandler{release: release})
	defer conn.Close()
	handle.Finish("closed")
	if observation.finishes.Load() != 0 {
		close(release)
		t.Fatal("dispatch finished before chained goroutine returned")
	}
	close(release)
	select {
	case <-observation.done:
	case <-time.After(time.Second):
		t.Fatal("chained dispatch did not release observation")
	}
	if observation.finishes.Load() != 1 {
		t.Fatal("chained dispatch finished twice")
	}
}
