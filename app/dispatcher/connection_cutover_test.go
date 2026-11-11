package dispatcher_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
)

type cutoverObservation struct {
	calls                        atomic.Int64
	upload                       atomic.Int64
	download                     atomic.Int64
	finishes                     atomic.Int64
	incomplete                   atomic.Bool
	state                        atomic.Value
	uploadEntered, uploadRelease chan struct{}
}

func (o *cutoverObservation) Routed(session.ConnectionRoute) { o.calls.Add(1) }
func (o *cutoverObservation) Destination(net.Destination)    { o.calls.Add(1) }
func (o *cutoverObservation) AddUpload(n int64) {
	if o.uploadEntered != nil {
		close(o.uploadEntered)
		<-o.uploadRelease
	}
	o.calls.Add(1)
	o.upload.Add(n)
}
func (o *cutoverObservation) AddDownload(n int64)      { o.calls.Add(1); o.download.Add(n) }
func (o *cutoverObservation) SetDownloadDeferred(bool) { o.calls.Add(1) }
func (o *cutoverObservation) MarkBytesIncomplete()     { o.calls.Add(1); o.incomplete.Store(true) }
func (o *cutoverObservation) Finish(state string) {
	o.calls.Add(1)
	o.state.Store(state)
	o.finishes.Add(1)
}

type cutoverObserver struct {
	mu                       sync.Mutex
	captures                 []*cutoverObservation
	openEntered, openRelease chan struct{}
	next                     *cutoverObservation
}

func (o *cutoverObserver) Open(session.ConnectionMetadata) session.ConnectionObservation {
	if o.openEntered != nil {
		close(o.openEntered)
		<-o.openRelease
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	capture := o.next
	if capture == nil {
		capture = new(cutoverObservation)
	}
	o.next = nil
	o.captures = append(o.captures, capture)
	return capture
}
func (o *cutoverObserver) snapshot() []*cutoverObservation {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]*cutoverObservation(nil), o.captures...)
}

func cutoverEcho(_ context.Context, link *transport.Link) {
	defer common.Close(link.Writer)
	defer common.Interrupt(link.Reader)
	for {
		mb, err := link.Reader.ReadMultiBuffer()
		if err != nil {
			return
		}
		if err := link.Writer.WriteMultiBuffer(mb); err != nil {
			return
		}
	}
}

func cutoverRoundTrip(t *testing.T, link *transport.Link, destination net.Destination) {
	t.Helper()
	packet := buf.FromBytes([]byte("cutover"))
	if destination.Network == net.Network_UDP {
		packet.UDP = &destination
	}
	if err := link.Writer.WriteMultiBuffer(buf.MultiBuffer{packet}); err != nil {
		t.Fatal(err)
	}
	mb, err := link.Reader.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	defer buf.ReleaseMulti(mb)
	if mb.Len() != 7 {
		t.Fatal("forwarding lost payload across observer cutover")
	}
	if destination.Network == net.Network_UDP && (len(mb) != 1 || mb[0].UDP == nil || *mb[0].UDP != destination) {
		t.Fatal("cutover lost UDP association destination")
	}
}

func closeCutoverLink(link *transport.Link) {
	common.Close(link.Writer)
	common.Interrupt(link.Reader)
}

func TestConnectionHotCutoverPreservesTCPAndUDPForwarding(t *testing.T) {
	for _, network := range []net.Network{net.Network_TCP, net.Network_UDP} {
		t.Run(network.String(), func(t *testing.T) {
			d, _ := flowDispatcher(t, &flowHandler{run: cutoverEcho}, nil)
			first, second := new(cutoverObserver), new(cutoverObserver)
			d.SetConnectionObserver(first)
			destination := net.Destination{Network: network, Address: net.LocalHostIP, Port: 80}
			old, err := d.Dispatch(context.Background(), destination)
			if err != nil {
				t.Fatal(err)
			}
			defer closeCutoverLink(old)
			cutoverRoundTrip(t, old, destination)
			capture := first.snapshot()[0]
			d.SetConnectionObserver(nil)
			if capture.finishes.Load() != 1 || capture.state.Load() != "interrupted" || !capture.incomplete.Load() {
				t.Fatal("active observation did not finalize interrupted/incomplete exactly once")
			}
			frozen := capture.calls.Load()
			off, err := d.Dispatch(context.Background(), destination)
			if err != nil {
				t.Fatal(err)
			}
			defer closeCutoverLink(off)
			cutoverRoundTrip(t, old, destination)
			cutoverRoundTrip(t, off, destination)
			d.SetConnectionObserver(second)
			cutoverRoundTrip(t, old, destination)
			cutoverRoundTrip(t, off, destination)
			if capture.calls.Load() != frozen || len(second.snapshot()) != 0 {
				t.Fatal("old or disabled flow resumed analytics after reenable")
			}
			fresh, err := d.Dispatch(context.Background(), destination)
			if err != nil {
				t.Fatal(err)
			}
			defer closeCutoverLink(fresh)
			cutoverRoundTrip(t, fresh, destination)
			newCapture := second.snapshot()[0]
			if newCapture.upload.Load() != 7 || newCapture.download.Load() != 7 {
				t.Fatal("new capture lost exact bytes")
			}
			d.SetConnectionObserver(nil)
			if capture.calls.Load() != frozen || capture.finishes.Load() != 1 {
				t.Fatal("old capture finalized twice")
			}
		})
	}
}

func TestConnectionObserverCutoverDrainsOpenAdmission(t *testing.T) {
	d, _ := flowDispatcher(t, &flowHandler{run: cutoverEcho}, nil)
	observer := &cutoverObserver{openEntered: make(chan struct{}), openRelease: make(chan struct{})}
	d.SetConnectionObserver(observer)
	dispatched := make(chan *transport.Link, 1)
	go func() {
		link, _ := d.Dispatch(context.Background(), net.TCPDestination(net.LocalHostIP, 80))
		dispatched <- link
	}()
	<-observer.openEntered
	cutoff := make(chan struct{})
	go func() { d.SetConnectionObserver(nil); close(cutoff) }()
	select {
	case <-cutoff:
		t.Fatal("cutoff escaped pending observer admission")
	case <-time.After(20 * time.Millisecond):
	}
	close(observer.openRelease)
	select {
	case <-cutoff:
	case <-time.After(5 * time.Second):
		t.Fatal("cutoff waited for forwarding completion")
	}
	link := <-dispatched
	defer closeCutoverLink(link)
	capture := observer.snapshot()[0]
	frozen := capture.calls.Load()
	cutoverRoundTrip(t, link, net.TCPDestination(net.LocalHostIP, 80))
	if capture.finishes.Load() != 1 || !capture.incomplete.Load() || capture.calls.Load() != frozen {
		t.Fatal("pending admission escaped cutoff")
	}
}

func TestConnectionObserverCutoverDrainsPendingCallback(t *testing.T) {
	d, _ := flowDispatcher(t, &flowHandler{run: cutoverEcho}, nil)
	capture := &cutoverObservation{uploadEntered: make(chan struct{}), uploadRelease: make(chan struct{})}
	d.SetConnectionObserver(&cutoverObserver{next: capture})
	destination := net.TCPDestination(net.LocalHostIP, 80)
	link, err := d.Dispatch(context.Background(), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer closeCutoverLink(link)
	writeDone := make(chan error, 1)
	go func() { writeDone <- link.Writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("cutover"))}) }()
	<-capture.uploadEntered
	cutoff := make(chan struct{})
	go func() { d.SetConnectionObserver(nil); close(cutoff) }()
	select {
	case <-cutoff:
		t.Fatal("cutoff returned with observer callback running")
	case <-time.After(20 * time.Millisecond):
	}
	close(capture.uploadRelease)
	select {
	case <-cutoff:
	case <-time.After(5 * time.Second):
		t.Fatal("callback drain did not finish")
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	mb, err := link.Reader.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	buf.ReleaseMulti(mb)
	frozen := capture.calls.Load()
	cutoverRoundTrip(t, link, destination)
	if capture.calls.Load() != frozen || capture.finishes.Load() != 1 {
		t.Fatal("callback continued after synchronous cutoff")
	}
}

func TestConnectionObserverConcurrentReplacement(t *testing.T) {
	d, _ := flowDispatcher(t, &flowHandler{run: cutoverEcho}, nil)
	const workers = 8
	observers := make([]*cutoverObserver, workers)
	links := make(chan *transport.Link, workers*24)
	var wg sync.WaitGroup
	for worker := range workers {
		observer := new(cutoverObserver)
		observers[worker] = observer
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 24 {
				d.SetConnectionObserver(observer)
				link, _ := d.Dispatch(context.Background(), net.TCPDestination(net.LocalHostIP, 80))
				links <- link
				d.SetConnectionObserver(nil)
			}
		}()
	}
	wg.Wait()
	d.SetConnectionObserver(nil)
	close(links)
	for link := range links {
		closeCutoverLink(link)
	}
	for _, observer := range observers {
		for _, capture := range observer.snapshot() {
			if capture.finishes.Load() != 1 || capture.state.Load() != "interrupted" || !capture.incomplete.Load() {
				t.Fatal("concurrent replacement orphaned a capture")
			}
		}
	}
}

func TestConnectionDisabledDelayedNestedDispatchNeverReattaches(t *testing.T) {
	resume := make(chan struct{})
	entered := make(chan struct{}, 2)
	var dispatchChild func(context.Context, net.Destination) (*transport.Link, error)
	handler := &flowHandler{run: func(ctx context.Context, link *transport.Link) {
		outbounds := session.OutboundsFromContext(ctx)
		if len(outbounds) > 1 {
			cutoverEcho(ctx, link)
			return
		}
		entered <- struct{}{}
		<-resume
		childCtx := session.ContextWithOutbounds(ctx, append(outbounds, &session.Outbound{}))
		child, err := dispatchChild(childCtx, outbounds[0].Target)
		if err != nil {
			common.Interrupt(link.Reader)
			common.Close(link.Writer)
			return
		}
		defer closeCutoverLink(child)
		defer common.Close(link.Writer)
		defer common.Interrupt(link.Reader)
		uploaded := make(chan struct{})
		go func() { buf.Copy(link.Reader, child.Writer); common.Close(child.Writer); close(uploaded) }()
		buf.Copy(child.Reader, link.Writer)
		<-uploaded
	}}
	d, _ := flowDispatcher(t, handler, nil)
	dispatchChild = d.Dispatch
	d.SetConnectionObserver(nil)
	destination := net.TCPDestination(net.LocalHostIP, 80)
	old, err := d.Dispatch(context.Background(), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer closeCutoverLink(old)
	<-entered
	observer := new(cutoverObserver)
	d.SetConnectionObserver(observer)
	close(resume)
	cutoverRoundTrip(t, old, destination)
	if len(observer.snapshot()) != 0 {
		t.Fatal("delayed child fabricated an observation for disabled application flow")
	}
	fresh, err := d.Dispatch(context.Background(), destination)
	if err != nil {
		t.Fatal(err)
	}
	defer closeCutoverLink(fresh)
	cutoverRoundTrip(t, fresh, destination)
	if len(observer.snapshot()) != 1 {
		t.Fatal("independent new application flow did not resume capture")
	}
	d.SetConnectionObserver(nil)
}
