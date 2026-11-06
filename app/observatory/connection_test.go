package observatory

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/session"
)

type probeConnectionObserver struct{ opens atomic.Int64 }

func (o *probeConnectionObserver) Open(session.ConnectionMetadata) session.ConnectionObservation {
	o.opens.Add(1)
	return nil
}

func TestProbeExplicitlyExcludesConnectionAnalytics(t *testing.T) {
	response := probeTestResponse(t, http.StatusNoContent)
	member := &probeTestHandler{tag: "member", handle: func(ctx context.Context, conn net.Conn) {
		if !session.ConnectionObservationDisabledFromContext(ctx) || session.ConnectionObservationFromContext(ctx) != nil {
			t.Error("internal probe retained application observation")
		}
		response(ctx, conn)
	}}
	ctx, dispatcher, _ := newProbeTestContext(t, member)
	observer := new(probeConnectionObserver)
	dispatcher.SetConnectionObserver(observer)
	if result := Probe(ctx, dispatcher, member.tag, "http://probe.invalid/health", time.Second); !result.Alive {
		t.Fatal(result.LastErrorReason)
	}
	if observer.opens.Load() != 0 {
		t.Fatal("health probe opened application analytics record")
	}
}
