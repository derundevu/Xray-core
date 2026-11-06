package dispatcher_test

import (
	"bytes"
	"context"
	"io"
	stdnet "net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/proxyman"
	outboundmanager "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
)

type flowObservation struct {
	mu               sync.Mutex
	opens            atomic.Int64
	incomplete       atomic.Bool
	metadata         session.ConnectionMetadata
	route            session.ConnectionRoute
	routes           int
	destinations     []net.Destination
	upload, download int64
	state            string
	done             chan struct{}
}

func (o *flowObservation) Open(metadata session.ConnectionMetadata) session.ConnectionObservation {
	o.opens.Add(1)
	o.metadata = metadata
	return o
}
func (o *flowObservation) Routed(route session.ConnectionRoute) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.route = route
	o.routes++
}
func (o *flowObservation) Destination(destination net.Destination) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.destinations = append(o.destinations, destination)
}
func (o *flowObservation) AddUpload(n int64)      { o.mu.Lock(); defer o.mu.Unlock(); o.upload += n }
func (o *flowObservation) AddDownload(n int64)    { o.mu.Lock(); defer o.mu.Unlock(); o.download += n }
func (o *flowObservation) MarkBytesIncomplete()   { o.incomplete.Store(true) }
func (*flowObservation) SetDownloadDeferred(bool) {}
func (o *flowObservation) Finish(state string)    { o.state = state; close(o.done) }
func (o *flowObservation) wait(t *testing.T) {
	t.Helper()
	select {
	case <-o.done:
	case <-time.After(5 * time.Second):
		t.Fatal("flow did not finish")
	}
}

type flowHandler struct {
	run func(context.Context, *transport.Link)
}

func (*flowHandler) Start() error                                         { return nil }
func (*flowHandler) Close() error                                         { return nil }
func (*flowHandler) Tag() string                                          { return "selected" }
func (*flowHandler) SenderSettings() *serial.TypedMessage                 { return nil }
func (*flowHandler) ProxySettings() *serial.TypedMessage                  { return nil }
func (h *flowHandler) Dispatch(ctx context.Context, link *transport.Link) { h.run(ctx, link) }

type flowRoute struct{ routing.Route }

func (flowRoute) GetOutboundTag() string { return "selected" }
func (flowRoute) GetRuleTag() string     { return "" }

type flowRouter struct {
	routing.Router
	entered, proceed chan struct{}
}

func (r *flowRouter) PickRoute(routing.Context) (routing.Route, error) {
	close(r.entered)
	<-r.proceed
	return flowRoute{}, nil
}
func flowDispatcher(t *testing.T, handler *flowHandler, router routing.Router) (*dispatcher.DefaultDispatcher, *flowObservation) {
	t.Helper()
	manager, err := outboundmanager.New(context.Background(), &proxyman.OutboundConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if handler != nil {
		if err := manager.AddHandler(context.Background(), handler); err != nil {
			t.Fatal(err)
		}
	}
	d := new(dispatcher.DefaultDispatcher)
	if err := d.Init(&dispatcher.Config{}, manager, router, nil, nil); err != nil {
		t.Fatal(err)
	}
	o := &flowObservation{done: make(chan struct{})}
	d.SetConnectionObserver(o)
	return d, o
}

func TestConnectionOpenBeforeSniffAndFinalRouteSnapshot(t *testing.T) {
	router := &flowRouter{entered: make(chan struct{}), proceed: make(chan struct{})}
	d, observation := flowDispatcher(t, &flowHandler{run: func(_ context.Context, link *transport.Link) {
		mb, err := link.Reader.ReadMultiBuffer()
		if err == nil {
			link.Writer.WriteMultiBuffer(mb)
		}
		common.Close(link.Writer)
		common.Interrupt(link.Reader)
	}}, router)
	ob := &session.Outbound{}
	ctx := session.ContextWithOutbounds(context.WithValue(context.Background(), core.XrayKey(1), new(core.Instance)), []*session.Outbound{ob})
	ctx = session.ContextWithContent(ctx, &session.Content{SniffingRequest: session.SniffingRequest{Enabled: true, RouteOnly: true, OverrideDestinationForProtocol: []string{"http"}}})
	destination := net.TCPDestination(net.LocalHostIP, 80)
	link, err := d.Dispatch(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	if observation.metadata.Destination != destination {
		t.Fatal("observation did not begin synchronously before sniff")
	}
	payload := []byte("GET / HTTP/1.1\r\nHost: example.test\r\n\r\n")
	if err := link.Writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(payload)}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-router.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("sniff/route did not start")
	}
	observation.mu.Lock()
	before := observation.routes
	observation.mu.Unlock()
	if before != 0 {
		t.Fatal("published route before router selected handler")
	}
	close(router.proceed)
	mb, err := link.Reader.ReadMultiBuffer()
	if err != nil {
		t.Fatal(err)
	}
	buf.ReleaseMulti(mb)
	observation.wait(t)
	ob.Target = net.TCPDestination(net.LocalHostIP, 9999)
	if observation.routes != 1 || observation.route.OutboundTag != "selected" || observation.route.Target != destination || observation.route.Domain != "example.test" {
		t.Fatalf("wrong immutable route: %+v", observation.route)
	}
	if observation.upload != int64(len(payload)) || observation.download != int64(len(payload)) {
		t.Fatal("sniff replay counted bytes twice")
	}
}

func TestConnectionActualTCPHalfCloseBytes(t *testing.T) {
	listener, err := stdnet.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		request, err := io.ReadAll(conn)
		if err == nil {
			conn.Write(append([]byte("response:"), request...))
		}
	}()
	d, observation := flowDispatcher(t, &flowHandler{run: func(ctx context.Context, link *transport.Link) {
		defer common.Close(link.Writer)
		defer common.Interrupt(link.Reader)
		conn, err := stdnet.DialTimeout("tcp", listener.Addr().String(), time.Second)
		if err != nil {
			session.RecordConnectionError(ctx, err)
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		if err := buf.Copy(link.Reader, buf.NewWriter(conn)); err != nil {
			session.RecordConnectionError(ctx, err)
			return
		}
		conn.(*stdnet.TCPConn).CloseWrite()
		session.RecordConnectionError(ctx, buf.Copy(buf.NewReader(conn), link.Writer))
	}}, nil)
	link, err := d.Dispatch(context.Background(), net.DestinationFromAddr(listener.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("application request")
	if err := link.Writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(payload)}); err != nil {
		t.Fatal(err)
	}
	if err := common.Close(link.Writer); err != nil {
		t.Fatal(err)
	}
	var response bytes.Buffer
	if err := buf.Copy(link.Reader, buf.NewWriter(&response)); err != nil {
		t.Fatal(err)
	}
	observation.wait(t)
	if response.String() != "response:"+string(payload) || observation.upload != int64(len(payload)) || observation.download != int64(response.Len()) || observation.state != "closed" {
		t.Fatal("half-close lost response, bytes, or normal completion")
	}
}

func TestConnectionMissingHandlerAttemptIsError(t *testing.T) {
	d, observation := flowDispatcher(t, nil, nil)
	ctx := session.SetForcedOutboundTagToContext(context.Background(), "missing")
	link, err := d.Dispatch(ctx, net.TCPDestination(net.LocalHostIP, 80))
	if err != nil {
		t.Fatal(err)
	}
	common.Close(link.Writer)
	observation.wait(t)
	if observation.state != "error" {
		t.Fatalf("missing handler became %s", observation.state)
	}
	if observation.route.OutboundTag != "missing" {
		t.Fatal("lost rejected route identity")
	}
}

func TestConnectionActualUDPAssociationDestinations(t *testing.T) {
	destinations := make([]net.Destination, 0, 2)
	for range 2 {
		server, err := stdnet.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer server.Close()
		server.SetDeadline(time.Now().Add(5 * time.Second))
		destinations = append(destinations, net.DestinationFromAddr(server.LocalAddr()))
		go func() {
			var packet [64]byte
			n, source, err := server.ReadFrom(packet[:])
			if err == nil {
				server.WriteTo(packet[:n], source)
			}
		}()
	}
	d, observation := flowDispatcher(t, &flowHandler{run: func(ctx context.Context, link *transport.Link) {
		defer common.Close(link.Writer)
		defer common.Interrupt(link.Reader)
		socket, err := stdnet.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			session.RecordConnectionError(ctx, err)
			return
		}
		defer socket.Close()
		socket.SetDeadline(time.Now().Add(5 * time.Second))
		packets, err := link.Reader.ReadMultiBuffer()
		if err != nil {
			session.RecordConnectionError(ctx, err)
			return
		}
		defer buf.ReleaseMulti(packets)
		for _, packet := range packets {
			if packet.UDP == nil {
				session.SetConnectionOutcome(ctx, "error")
				return
			}
			if _, err := socket.WriteTo(packet.Bytes(), packet.UDP.RawNetAddr()); err != nil {
				session.RecordConnectionError(ctx, err)
				return
			}
		}
		for range packets {
			var payload [64]byte
			n, source, err := socket.ReadFrom(payload[:])
			if err != nil {
				session.RecordConnectionError(ctx, err)
				return
			}
			packet := buf.FromBytes(payload[:n])
			destination := net.DestinationFromAddr(source)
			packet.UDP = &destination
			if err := link.Writer.WriteMultiBuffer(buf.MultiBuffer{packet}); err != nil {
				session.RecordConnectionError(ctx, err)
				return
			}
		}
	}}, nil)
	link, err := d.Dispatch(context.Background(), destinations[0])
	if err != nil {
		t.Fatal(err)
	}
	packets := buf.MultiBuffer{buf.FromBytes([]byte("one")), buf.FromBytes([]byte("two"))}
	packets[0].UDP, packets[1].UDP = &destinations[0], &destinations[1]
	if err := link.Writer.WriteMultiBuffer(packets); err != nil {
		t.Fatal(err)
	}
	var received buf.MultiBuffer
	for len(received) < 2 {
		mb, err := link.Reader.ReadMultiBuffer()
		if err != nil {
			t.Fatal(err)
		}
		received = append(received, mb...)
	}
	defer buf.ReleaseMulti(received)
	observation.wait(t)
	if observation.metadata.Network != net.Network_UDP || observation.metadata.Destination != destinations[0] || observation.upload != 6 || observation.download != 6 {
		t.Fatal("lost UDP association identity or bytes")
	}
	foundSecond := false
	for _, destination := range observation.destinations {
		if destination == destinations[1] {
			foundSecond = true
		}
	}
	if !foundSecond {
		t.Fatal("association falsely reported only its first destination")
	}
	for _, packet := range received {
		if packet.UDP == nil || (*packet.UDP != destinations[0] && *packet.UDP != destinations[1]) {
			t.Fatal("lost per-packet UDP source")
		}
	}
}

func TestConnectionNestedDispatchCountsLogicalFlowOnce(t *testing.T) {
	var d *dispatcher.DefaultDispatcher
	handler := &flowHandler{run: func(ctx context.Context, link *transport.Link) {
		defer common.Close(link.Writer)
		defer common.Interrupt(link.Reader)
		outbounds := session.OutboundsFromContext(ctx)
		if len(outbounds) > 1 {
			session.RecordConnectionError(ctx, buf.Copy(link.Reader, link.Writer))
			return
		}
		childCtx := session.ContextWithOutbounds(ctx, append(outbounds, &session.Outbound{}))
		child, err := d.Dispatch(childCtx, outbounds[0].Target)
		if err != nil {
			session.RecordConnectionError(ctx, err)
			return
		}
		uploaded := make(chan error, 1)
		go func() {
			err := buf.Copy(link.Reader, child.Writer)
			common.Close(child.Writer)
			uploaded <- err
		}()
		session.RecordConnectionError(ctx, buf.Copy(child.Reader, link.Writer))
		session.RecordConnectionError(ctx, <-uploaded)
	}}
	d, observation := flowDispatcher(t, handler, nil)
	link, err := d.Dispatch(context.Background(), net.TCPDestination(net.LocalHostIP, 80))
	if err != nil {
		t.Fatal(err)
	}
	if err := link.Writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("logical"))}); err != nil {
		t.Fatal(err)
	}
	common.Close(link.Writer)
	var response bytes.Buffer
	if err := buf.Copy(link.Reader, buf.NewWriter(&response)); err != nil {
		t.Fatal(err)
	}
	observation.wait(t)
	if observation.opens.Load() != 1 || observation.routes != 1 || observation.upload != 7 || observation.download != 7 || response.String() != "logical" {
		t.Fatal("child dispatch duplicated logical analytics")
	}
}
