package proxy_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/proxy"
)

type spliceWriteSignalConn struct {
	net.Conn
	started chan struct{}
}

func (c *spliceWriteSignalConn) Write(payload []byte) (int, error) {
	close(c.started)
	return c.Conn.Write(payload)
}

func TestVisionWriterPublishesSpliceReadinessAfterWrite(t *testing.T) {
	for _, test := range []struct {
		name              string
		initial           int32
		forbidDuringWrite bool
		failWrite         bool
		want              int32
	}{
		{name: "pending becomes ready", initial: 2, want: 1},
		{name: "failed write stays pending", initial: 2, failWrite: true, want: 2},
		{name: "forbidden during write stays forbidden", initial: 2, forbidDuringWrite: true, want: 3},
		{name: "unspecified stays unspecified", initial: 0, want: 0},
		{name: "ready stays ready", initial: 1, want: 1},
		{name: "forbidden stays forbidden", initial: 3, want: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, peer := net.Pipe()
			defer conn.Close()
			defer peer.Close()
			deadline := time.Now().Add(5 * time.Second)
			if err := conn.SetDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			if err := peer.SetDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			signaled := &spliceWriteSignalConn{Conn: conn, started: make(chan struct{})}
			inbound := &session.Inbound{}
			inbound.CanSpliceCopy.Store(test.initial)
			ctx := session.ContextWithInbound(context.Background(), inbound)
			state := &proxy.TrafficState{
				Inbound: proxy.InboundState{DownlinkWriterDirectCopy: true},
			}
			writer := proxy.NewVisionWriter(buf.NewWriter(signaled), state, false, ctx, signaled, nil, nil)
			payload := []byte("final protocol write before splice")
			written := make(chan error, 1)
			go func() {
				written <- writer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(payload)})
			}()
			select {
			case <-signaled.started:
			case <-time.After(5 * time.Second):
				t.Fatal("direct write did not start")
			}

			// net.Pipe cannot complete the write until its peer reads the payload.
			if got := inbound.CanSpliceCopy.Load(); got != test.initial {
				t.Fatalf("splice state changed before the write completed: got %d, want %d", got, test.initial)
			}
			if test.forbidDuringWrite {
				inbound.CanSpliceCopy.Store(3)
			}
			if test.failWrite {
				peer.Close()
			} else {
				received := make([]byte, len(payload))
				if _, err := io.ReadFull(peer, received); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(received, payload) {
					t.Fatalf("direct write changed payload: got %q, want %q", received, payload)
				}
			}
			select {
			case err := <-written:
				if (err != nil) != test.failWrite {
					t.Fatalf("direct write returned %v, want failure %t", err, test.failWrite)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("direct write did not finish")
			}
			if got := inbound.CanSpliceCopy.Load(); got != test.want {
				t.Fatalf("splice state after write: got %d, want %d", got, test.want)
			}
		})
	}
}
