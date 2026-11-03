package internet_test

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/xtls/xray-core/transport/internet"
	"golang.org/x/sys/unix"
)

func listenerSocketProtocol(t *testing.T, listener net.Listener) int {
	t.Helper()
	tcpListener, ok := listener.(*net.TCPListener)
	if !ok {
		t.Fatalf("expected TCP listener, got %T", listener)
	}
	raw, err := tcpListener.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var protocol int
	var socketErr error
	if err := raw.Control(func(fd uintptr) {
		protocol, socketErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_PROTOCOL)
	}); err != nil {
		t.Fatal(err)
	}
	if socketErr != nil {
		t.Fatal(socketErr)
	}
	return protocol
}

func TestDefaultListenerMultipathTCPOptIn(t *testing.T) {
	// Force Go's ambient MPTCP default on: Xray must still require tcpMptcp.
	t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",multipathtcp=3")

	var optIn net.ListenConfig
	optIn.SetMultipathTCP(true)
	reference, err := optIn.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reference.Close() })
	optInProtocol := listenerSocketProtocol(t, reference)
	if optInProtocol == unix.IPPROTO_TCP {
		t.Log("kernel uses Go's TCP fallback for MPTCP; opt-in must retain that behavior")
	}

	for _, tc := range []struct {
		name     string
		sockopt  *internet.SocketConfig
		protocol int
	}{
		{name: "nil socket options", protocol: unix.IPPROTO_TCP},
		{name: "default socket options", sockopt: &internet.SocketConfig{}, protocol: unix.IPPROTO_TCP},
		{name: "explicitly disabled", sockopt: &internet.SocketConfig{TcpMptcp: false}, protocol: unix.IPPROTO_TCP},
		{name: "explicitly enabled", sockopt: &internet.SocketConfig{TcpMptcp: true}, protocol: optInProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listener := internet.DefaultListener{}
			conn, err := listener.Listen(context.Background(), &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}, tc.sockopt)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			if got := listenerSocketProtocol(t, conn); got != tc.protocol {
				t.Fatalf("listener socket protocol = %d, want %d", got, tc.protocol)
			}
		})
	}
}
