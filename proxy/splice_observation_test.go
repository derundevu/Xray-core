package proxy_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal"
	"github.com/xtls/xray-core/common/task"
	"github.com/xtls/xray-core/proxy"
)

type spliceObservation struct {
	download atomic.Int64
	deferred atomic.Bool
	started  chan struct{}
	finishes atomic.Int64
	finished chan struct{}
	state    string
}

func (*spliceObservation) Routed(session.ConnectionRoute) {}
func (*spliceObservation) Destination(xnet.Destination)   {}
func (*spliceObservation) AddUpload(int64)                {}
func (o *spliceObservation) AddDownload(n int64)          { o.download.Add(n) }
func (*spliceObservation) MarkBytesIncomplete()           {}
func (o *spliceObservation) SetDownloadDeferred(deferred bool) {
	o.deferred.Store(deferred)
	if deferred {
		close(o.started)
	}
}
func (o *spliceObservation) Finish(state string) {
	o.state = state
	o.finishes.Add(1)
	if o.finished != nil {
		close(o.finished)
	}
}

func spliceTCPPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	peer, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := listener.AcceptTCP()
	if err != nil {
		peer.Close()
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	conn.SetDeadline(deadline)
	peer.SetDeadline(deadline)
	t.Cleanup(func() { conn.Close(); peer.Close() })
	return conn, peer
}
func TestSpliceObservationDeferredAndExactFinalBytes(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "android" {
		t.Skip("kernel splice is Linux/Android only")
	}
	upstream, upstreamPeer := spliceTCPPair(t)
	downstream, downstreamPeer := spliceTCPPair(t)
	inbound, outbound := &session.Inbound{}, &session.Outbound{}
	inbound.CanSpliceCopy.Store(1)
	outbound.CanSpliceCopy.Store(1)
	observation := &spliceObservation{started: make(chan struct{})}
	ctx := session.ContextWithInbound(context.Background(), inbound)
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{outbound})
	ctx = session.ContextWithConnectionObservation(ctx, observation)
	timer := signal.CancelAfterInactivity(ctx, func() {}, time.Minute)
	defer timer.SetTimeout(0)
	done := make(chan error, 1)
	go func() { done <- proxy.CopyRawConnIfExist(ctx, upstream, downstream, buf.Discard, timer, nil) }()
	select {
	case <-observation.started:
	case <-time.After(5 * time.Second):
		t.Fatal("splice was disabled or not marked deferred")
	}
	payload := bytes.Repeat([]byte("application"), 4096)
	written := make(chan error, 1)
	go func() { _, err := upstreamPeer.Write(payload); written <- err }()
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(downstreamPeer, got); err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("splice changed application payload")
	}
	if !observation.deferred.Load() || observation.download.Load() != 0 {
		t.Fatal("blocking splice advertised exact live bytes")
	}
	if err := upstreamPeer.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("splice did not finish after half-close")
	}
	if observation.deferred.Load() || observation.download.Load() != int64(len(payload)) {
		t.Fatalf("splice final count %d, deferred %v", observation.download.Load(), observation.deferred.Load())
	}
}

func TestSpliceObservationSurvivesTaskEarlyReturn(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "android" {
		t.Skip("kernel splice is Linux/Android only")
	}
	for _, canceled := range []bool{false, true} {
		name := "upload failure"
		if canceled {
			name = "cancellation"
		}
		t.Run(name, func(t *testing.T) {
			upstream, upstreamPeer := spliceTCPPair(t)
			downstream, downstreamPeer := spliceTCPPair(t)
			inbound, outbound := &session.Inbound{}, &session.Outbound{}
			inbound.CanSpliceCopy.Store(1)
			outbound.CanSpliceCopy.Store(1)
			observation := &spliceObservation{started: make(chan struct{}), finished: make(chan struct{})}
			handle := session.NewConnectionObservationHandle(observation)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			handle.BindContext(ctx)
			ctx = session.ContextWithInbound(ctx, inbound)
			ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{outbound})
			ctx = session.ContextWithConnectionObservation(ctx, handle)
			timer := signal.CancelAfterInactivity(ctx, func() {}, time.Minute)
			defer timer.SetTimeout(0)
			returned := make(chan error, 1)
			go func() {
				err := task.Run(ctx, func() error {
					<-observation.started
					if canceled {
						cancel()
						return nil
					}
					return errors.New("synthetic upload failure")
				}, func() error {
					return proxy.CopyRawConnIfExist(ctx, upstream, downstream, buf.Discard, timer, nil)
				})
				session.RecordConnectionError(ctx, err)
				handle.Finish("closed")
				returned <- err
			}()
			select {
			case err := <-returned:
				if err == nil {
					t.Fatal("Run did not return early")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Run waited for blocked splice")
			}
			if observation.finishes.Load() != 0 || !observation.deferred.Load() {
				t.Fatal("dispatch return finished blocked splice")
			}
			payload := []byte("download delivered after task.Run returned")
			if _, err := upstreamPeer.Write(payload); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(downstreamPeer, got); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("late splice payload changed")
			}
			if err := upstreamPeer.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-observation.finished:
			case <-time.After(5 * time.Second):
				t.Fatal("splice did not release lifecycle after final credit")
			}
			wantState := "error"
			if canceled {
				wantState = "interrupted"
			}
			if observation.state != wantState || observation.finishes.Load() != 1 || observation.deferred.Load() || observation.download.Load() != int64(len(payload)) {
				t.Fatalf("state %s; finishes %d; deferred %v; download %d", observation.state, observation.finishes.Load(), observation.deferred.Load(), observation.download.Load())
			}
		})
	}
}
