package observatory

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/app/proxyman"
	v2net "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/proxy/socks"
)

func TestProbeCancellationClosesStalledSOCKSHandshake(t *testing.T) {
	for _, stage := range []string{"greeting", "connect_response"} {
		for _, cancelRequest := range []bool{true, false} {
			name := stage + "/timeout"
			if cancelRequest {
				name = stage + "/cancellation"
			}
			t.Run(name, func(t *testing.T) {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				accepted := make(chan net.Conn, 1)
				started := make(chan struct{})
				peerResult := make(chan error, 1)
				t.Cleanup(func() {
					listener.Close()
					select {
					case conn := <-accepted:
						conn.Close()
					default:
					}
				})
				go func() {
					conn, err := listener.Accept()
					if err != nil {
						peerResult <- err
						return
					}
					accepted <- conn
					defer conn.Close()
					conn.SetReadDeadline(time.Now().Add(3 * time.Second))
					var greeting [3]byte
					if _, err := io.ReadFull(conn, greeting[:]); err != nil {
						peerResult <- fmt.Errorf("read SOCKS greeting: %w", err)
						return
					}
					if greeting != [3]byte{5, 1, 0} {
						peerResult <- fmt.Errorf("unexpected SOCKS greeting: %v", greeting)
						return
					}
					if stage == "connect_response" {
						if _, err := conn.Write([]byte{5, 0}); err != nil {
							peerResult <- err
							return
						}
						var header [5]byte
						if _, err := io.ReadFull(conn, header[:]); err != nil {
							peerResult <- fmt.Errorf("read SOCKS CONNECT header: %w", err)
							return
						}
						if header[0] != 5 || header[1] != 1 || header[2] != 0 || header[3] != 3 {
							peerResult <- fmt.Errorf("unexpected SOCKS CONNECT header: %v", header)
							return
						}
						target := make([]byte, int(header[4])+2)
						if _, err := io.ReadFull(conn, target); err != nil {
							peerResult <- fmt.Errorf("read SOCKS CONNECT target: %w", err)
							return
						}
						if string(target[:len(target)-2]) != "probe.invalid" {
							peerResult <- fmt.Errorf("unexpected SOCKS target: %v", target)
							return
						}
					}
					close(started)
					var b [1]byte
					_, err = conn.Read(b[:])
					if err != io.EOF {
						peerResult <- fmt.Errorf("stalled SOCKS connection did not close with EOF: %v", err)
						return
					}
					peerResult <- nil
				}()
				instance, err := core.New(&core.Config{
					App: []*serial.TypedMessage{
						serial.ToTypedMessage(&dispatcher.Config{}),
						serial.ToTypedMessage(&proxyman.OutboundConfig{}),
					},
					Outbound: []*core.OutboundHandlerConfig{{
						Tag: "member",
						ProxySettings: serial.ToTypedMessage(&socks.ClientConfig{Server: &protocol.ServerEndpoint{
							Address: v2net.NewIPOrDomain(v2net.LocalHostIP),
							Port:    uint32(listener.Addr().(*net.TCPAddr).Port),
						}}),
					}},
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
				case err := <-peerResult:
					t.Fatalf("SOCKS peer failed before handshake stalled: %v", err)
				case <-time.After(2 * time.Second):
					t.Fatal("probe did not reach SOCKS handshake")
				}
				if cancelRequest {
					cancel()
				}
				select {
				case got := <-result:
					if got.Alive || got.LastErrorReason == "" {
						t.Fatalf("stalled SOCKS probe reported success: alive=%v reason=%q", got.Alive, got.LastErrorReason)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("probe did not return during stalled SOCKS handshake")
				}
				select {
				case err := <-peerResult:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("SOCKS handshake connection outlived canceled probe")
				}
			})
		}
	}
}
