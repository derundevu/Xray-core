package outbound

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type cancellationCountingConnection struct {
	net.Conn
	closes   atomic.Int32
	closeErr error
}

func (c *cancellationCountingConnection) Close() error {
	c.closes.Add(1)
	c.Conn.Close()
	return c.closeErr
}

func TestCancellationBoundConnectionClosesOnContextCancellation(t *testing.T) {
	conn, peer := net.Pipe()
	defer peer.Close()
	raw := &cancellationCountingConnection{Conn: conn}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bound := newCancellationBoundConnection(ctx, raw)
	defer bound.Close()
	cancel()
	peer.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := peer.Read(b[:]); err != io.EOF {
		t.Fatalf("private connection outlived cancellation: %v", err)
	}
	var closes sync.WaitGroup
	for range 16 {
		closes.Go(func() { bound.Close() })
	}
	closes.Wait()
	if raw.closes.Load() != 1 {
		t.Fatalf("underlying connection closed %d times", raw.closes.Load())
	}
}

func TestCancellationBoundConnectionCloseUnregistersCallback(t *testing.T) {
	conn, peer := net.Pipe()
	defer peer.Close()
	closeErr := errors.New("close result")
	raw := &cancellationCountingConnection{Conn: conn, closeErr: closeErr}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bound := newCancellationBoundConnection(ctx, raw)
	if err := bound.Close(); err != closeErr {
		t.Fatalf("lost underlying close error: %v", err)
	}
	// The parent is not canceled, so a still-registered callback could not have
	// started. A successful stop here would mean Close retained that callback.
	if bound.stop() {
		t.Fatal("connection close did not unregister cancellation callback")
	}
	cancel()
	if err := bound.Close(); err != closeErr {
		t.Fatalf("repeated close lost underlying result: %v", err)
	}
	if raw.closes.Load() != 1 {
		t.Fatalf("underlying connection closed %d times", raw.closes.Load())
	}
}
