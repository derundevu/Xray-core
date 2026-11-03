package observatory

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	outboundmanager "github.com/xtls/xray-core/app/proxyman/outbound"
	v2net "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/net/cnc"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/policy"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/features/stats"
	"github.com/xtls/xray-core/transport"
	_ "github.com/xtls/xray-core/transport/internet/tagged/taggedimpl"
)

type probeTestHandler struct {
	tag    string
	calls  atomic.Int32
	handle func(context.Context, net.Conn)
}

func (h *probeTestHandler) Tag() string                          { return h.tag }
func (h *probeTestHandler) Start() error                         { return nil }
func (h *probeTestHandler) Close() error                         { return nil }
func (h *probeTestHandler) SenderSettings() *serial.TypedMessage { return nil }
func (h *probeTestHandler) ProxySettings() *serial.TypedMessage  { return nil }
func (h *probeTestHandler) Dispatch(ctx context.Context, link *transport.Link) {
	h.calls.Add(1)
	conn := cnc.NewConnection(cnc.ConnectionInputMulti(link.Writer), cnc.ConnectionOutputMulti(link.Reader))
	defer conn.Close()
	h.handle(ctx, conn)
}

func newProbeTestContext(t *testing.T, handlers ...*probeTestHandler) (context.Context, *dispatcher.DefaultDispatcher, *outboundmanager.Manager) {
	t.Helper()
	instance, err := core.New(&core.Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { instance.Close() })
	ctx := instance.Context()
	manager, err := outboundmanager.New(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, handler := range handlers {
		if err := manager.AddHandler(ctx, handler); err != nil {
			t.Fatal(err)
		}
	}
	if err := instance.AddFeature(manager); err != nil {
		t.Fatal(err)
	}
	d := new(dispatcher.DefaultDispatcher)
	if err := d.Init(&dispatcher.Config{}, manager, nil, policy.DefaultManager{}, stats.NoopManager{}); err != nil {
		t.Fatal(err)
	}
	return ctx, d, manager
}

func probeTestResponse(t *testing.T, status int) func(context.Context, net.Conn) {
	t.Helper()
	return func(ctx context.Context, conn net.Conn) {
		request, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			t.Errorf("read probe request: %v", err)
			return
		}
		request.Body.Close()
		if request.Method != http.MethodGet || request.Host != "probe.invalid" || request.URL.Path != "/health" {
			t.Errorf("unexpected probe request: %s %s %s", request.Method, request.Host, request.URL.Path)
		}
		if core.FromContext(ctx) == nil {
			t.Error("probe lost its Xray context")
		}
		fmt.Fprintf(conn, "HTTP/1.1 %d %s\r\nContent-Length: 0\r\nLocation: http://redirect.invalid/\r\nConnection: close\r\n\r\n", status, http.StatusText(status))
	}
}

func TestProbeUsesExactOutboundAndRequires2xx(t *testing.T) {
	for _, status := range []int{200, 204, 299, 301, 302, 404, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			fallback := &probeTestHandler{tag: "default", handle: probeTestResponse(t, http.StatusNoContent)}
			member := &probeTestHandler{tag: "member", handle: probeTestResponse(t, status)}
			ctx, d, _ := newProbeTestContext(t, fallback, member)
			result := Probe(ctx, d, member.tag, "http://probe.invalid/health", time.Second)
			wantAlive := status >= 200 && status < 300
			if result.Alive != wantAlive {
				t.Fatalf("status %d: alive=%v, reason=%q", status, result.Alive, result.LastErrorReason)
			}
			if !wantAlive && !strings.Contains(result.LastErrorReason, fmt.Sprint(status)) {
				t.Fatalf("missing failed response status: %q", result.LastErrorReason)
			}
			if member.calls.Load() != 1 || fallback.calls.Load() != 0 {
				t.Fatalf("wrong outbound or followed redirect: member=%d default=%d", member.calls.Load(), fallback.calls.Load())
			}
		})
	}
}

func TestProbeRejectsMissingOutboundWithoutDefault(t *testing.T) {
	fallback := &probeTestHandler{tag: "default", handle: probeTestResponse(t, http.StatusNoContent)}
	ctx, d, _ := newProbeTestContext(t, fallback)
	for _, tag := range []string{"", "missing"} {
		result := Probe(ctx, d, tag, "http://probe.invalid/health", time.Second)
		if result.Alive || result.LastErrorReason == "" {
			t.Fatalf("tag %q should fail explicitly: alive=%v reason=%q", tag, result.Alive, result.LastErrorReason)
		}
	}
	if fallback.calls.Load() != 0 {
		t.Fatal("missing outbound reached default handler")
	}
}

type probeRemovingDispatcher struct {
	routing.Dispatcher
	remove func()
}

func (d *probeRemovingDispatcher) Dispatch(ctx context.Context, destination v2net.Destination) (*transport.Link, error) {
	d.remove()
	return d.Dispatcher.Dispatch(ctx, destination)
}

func TestProbeRemovedOutboundCannotFallBackAfterValidation(t *testing.T) {
	fallback := &probeTestHandler{tag: "default", handle: probeTestResponse(t, http.StatusNoContent)}
	member := &probeTestHandler{tag: "member", handle: probeTestResponse(t, http.StatusNoContent)}
	ctx, d, manager := newProbeTestContext(t, fallback, member)
	removing := &probeRemovingDispatcher{Dispatcher: d, remove: func() {
		if err := manager.RemoveHandler(ctx, member.tag); err != nil {
			t.Errorf("remove member: %v", err)
		}
	}}
	result := Probe(ctx, removing, member.tag, "http://probe.invalid/health", time.Second)
	if result.Alive {
		t.Fatal("removed outbound was reported alive")
	}
	if fallback.calls.Load() != 0 || member.calls.Load() != 0 {
		t.Fatalf("removed forced tag reached a handler: member=%d default=%d", member.calls.Load(), fallback.calls.Load())
	}
}

func TestProbeCancellationAndTimeoutCloseDispatchedRequest(t *testing.T) {
	for _, cancelRequest := range []bool{true, false} {
		name := "timeout"
		if cancelRequest {
			name = "cancellation"
		}
		t.Run(name, func(t *testing.T) {
			started := make(chan struct{})
			closed := make(chan error, 1)
			member := &probeTestHandler{tag: "member", handle: func(ctx context.Context, conn net.Conn) {
				request, err := http.ReadRequest(bufio.NewReader(conn))
				if err != nil {
					closed <- fmt.Errorf("read probe request: %w", err)
					return
				}
				request.Body.Close()
				close(started)
				<-ctx.Done()
				var b [1]byte
				if _, err := conn.Read(b[:]); err == nil {
					closed <- fmt.Errorf("request connection remained readable after cancellation")
					return
				}
				closed <- nil
			}}
			ctx, d, _ := newProbeTestContext(t, member)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			timeout := 250 * time.Millisecond
			if cancelRequest {
				timeout = 5 * time.Second
			}
			result := make(chan ProbeResult, 1)
			go func() { result <- Probe(ctx, d, member.tag, "http://probe.invalid/health", timeout) }()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("probe did not reach selected outbound")
			}
			if cancelRequest {
				cancel()
			}
			select {
			case got := <-result:
				if got.Alive || got.LastErrorReason == "" {
					t.Fatalf("canceled request reported success: alive=%v reason=%q", got.Alive, got.LastErrorReason)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("probe did not return after cancellation or timeout")
			}
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("dispatched context or HTTP connection outlived probe")
			}
		})
	}
}

func TestStockObserverStillAcceptsCompletedHTTPErrorResponse(t *testing.T) {
	member := &probeTestHandler{tag: "member", handle: probeTestResponse(t, http.StatusServiceUnavailable)}
	ctx, d, _ := newProbeTestContext(t, member)
	observer := &Observer{ctx: ctx, dispatcher: d, config: &Config{ProbeUrl: "http://probe.invalid/health"}}
	result := observer.probe(member.tag)
	if !result.Alive {
		t.Fatalf("stock response semantics changed: %q", result.LastErrorReason)
	}
}
