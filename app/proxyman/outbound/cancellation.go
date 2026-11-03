package outbound

import (
	"context"
	"sync"

	"github.com/xtls/xray-core/transport/internet/stat"
)

// cancellationBoundConnection closes a private outbound connection when bounded
// work is canceled, including while a proxy performs synchronous handshake I/O.
// Its owner must not use it for transports shared by independent requests.
type cancellationBoundConnection struct {
	stat.Connection
	stop      func() bool
	closeOnce sync.Once
	closeErr  error
}

func newCancellationBoundConnection(ctx context.Context, conn stat.Connection) *cancellationBoundConnection {
	bound := &cancellationBoundConnection{Connection: conn}
	bound.stop = context.AfterFunc(ctx, func() { bound.close() })
	return bound
}

func (c *cancellationBoundConnection) Close() error {
	c.stop()
	return c.close()
}

func (c *cancellationBoundConnection) close() error {
	c.closeOnce.Do(func() { c.closeErr = c.Connection.Close() })
	return c.closeErr
}
