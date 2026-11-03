package observatory

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/proxyman"
	v2net "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/transport/internet"
	_ "github.com/xtls/xray-core/transport/internet/tcp"
)

func TestProbeCancellationReachesChainedFreedomOutbound(t *testing.T) {
	for _, cancelRequest := range []bool{true, false} {
		name := "timeout"
		if cancelRequest {
			name = "cancellation"
		}
		t.Run(name, func(t *testing.T) {
			started := make(chan struct{})
			canceled := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.Host != "probe.invalid" || request.URL.Path != "/health" {
					t.Errorf("unexpected chained probe request: %s %s", request.Host, request.URL.Path)
				}
				close(started)
				<-request.Context().Done()
				close(canceled)
			}))
			t.Cleanup(func() {
				server.CloseClientConnections()
				server.Close()
			})
			instance, err := core.New(&core.Config{
				App: []*serial.TypedMessage{
					serial.ToTypedMessage(&dispatcher.Config{}),
					serial.ToTypedMessage(&proxyman.OutboundConfig{}),
				},
				Outbound: []*core.OutboundHandlerConfig{
					{
						Tag: "member",
						SenderSettings: serial.ToTypedMessage(&proxyman.SenderConfig{
							StreamSettings: &internet.StreamConfig{
								ProtocolName:   "tcp",
								SocketSettings: &internet.SocketConfig{DialerProxy: "carrier"},
							},
						}),
						ProxySettings: serial.ToTypedMessage(&freedom.Config{}),
					},
					{
						Tag: "carrier",
						ProxySettings: serial.ToTypedMessage(&freedom.Config{
							DestinationOverride: &freedom.DestinationOverride{Server: &protocol.ServerEndpoint{
								Address: v2net.NewIPOrDomain(v2net.LocalHostIP),
								Port:    uint32(server.Listener.Addr().(*net.TCPAddr).Port),
							}},
						}),
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { instance.Close() })
			ctx, cancel := context.WithCancel(instance.Context())
			defer cancel()
			timeout := 250 * time.Millisecond
			if cancelRequest {
				timeout = 5 * time.Second
			}
			result := make(chan ProbeResult, 1)
			go func() {
				d := instance.GetFeature(routing.DispatcherType()).(routing.Dispatcher)
				result <- Probe(ctx, d, "member", "http://probe.invalid/health", timeout)
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("probe did not reach chained HTTP endpoint")
			}
			if cancelRequest {
				cancel()
			}
			select {
			case got := <-result:
				if got.Alive || got.LastErrorReason == "" {
					t.Fatalf("canceled chained probe reported success: alive=%v reason=%q", got.Alive, got.LastErrorReason)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("chained probe did not return within its lifetime")
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("chained outbound retained the canceled HTTP request")
			}
		})
	}
}
