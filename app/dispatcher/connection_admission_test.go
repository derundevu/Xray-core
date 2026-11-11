package dispatcher

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
)

func TestConnectionObserverDisabledAdmissionAllocations(t *testing.T) {
	d := new(DefaultDispatcher)
	// Every invocation is a fresh logical flow, not a repeated fast-path hit on
	// an already-disabled context. Content is required by Dispatch regardless
	// of analytics, and is prepared outside the measured admission boundary.
	contexts := make([]context.Context, 1001)
	for i := range contexts {
		contexts[i] = session.ContextWithContent(context.Background(), new(session.Content))
	}
	next := 0
	destination := net.TCPDestination(net.LocalHostIP, 80)
	allocations := testing.AllocsPerRun(1000, func() {
		ctx := contexts[next]
		next++
		got, handle := d.openConnection(ctx, destination)
		if got != ctx || handle != nil || session.ConnectionObservationFromContext(got) != nil || session.RetainConnectionObservation(got) != nil {
			t.Fatal("disabled admission installed an analytics context, handle or lifetime hook")
		}
		if !session.ConnectionObservationDisabledFromContext(got) {
			t.Fatal("disabled admission lost logical-flow fence")
		}
	})
	if allocations != 0 {
		t.Fatalf("disabled admission allocated %v objects per new flow", allocations)
	}
}
