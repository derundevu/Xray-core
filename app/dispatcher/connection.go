package dispatcher

import (
	"context"
	"sync"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

// ConnectionObserver belongs to one dispatcher instance. Callbacks must not call
// SetConnectionObserver: replacement waits synchronously for callbacks to drain.
type ConnectionObserver interface {
	Open(session.ConnectionMetadata) session.ConnectionObservation
}

type connectionObserverGeneration struct {
	mu       sync.Mutex
	observer ConnectionObserver
	handles  map[*session.ConnectionObservationHandle]struct{}
	finished func(*session.ConnectionObservationHandle)
}

func (g *connectionObserverGeneration) remove(handle *session.ConnectionObservationHandle) {
	g.mu.Lock()
	delete(g.handles, handle)
	g.mu.Unlock()
}

// SetConnectionObserver synchronously cuts off the previous generation, including
// pending Open calls. Active captures finish interrupted/incomplete, but their
// forwarding contexts, transports and retained async work are never stopped.
// A replacement observes only new logical connections.
func (d *DefaultDispatcher) SetConnectionObserver(observer ConnectionObserver) {
	d.observerMu.Lock()
	defer d.observerMu.Unlock()
	if old := d.connectionObserver.Swap(nil); old != nil {
		old.mu.Lock()
		old.observer = nil
		handles := old.handles
		old.handles = nil
		old.finished = nil
		old.mu.Unlock()
		for handle := range handles {
			handle.Interrupt()
		}
	}
	if observer != nil {
		next := &connectionObserverGeneration{
			observer: observer,
			handles:  make(map[*session.ConnectionObservationHandle]struct{}),
		}
		next.finished = next.remove
		d.connectionObserver.Store(next)
	}
}

func (d *DefaultDispatcher) openConnection(ctx context.Context, destination net.Destination) (context.Context, *session.ConnectionObservationHandle) {
	// The nil path constructs no metadata, handle, callback or context wrapper.
	generation := d.connectionObserver.Load()
	if session.ConnectionObservationDisabledFromContext(ctx) || session.ConnectionObservationFromContext(ctx) != nil {
		return ctx, nil
	}
	if generation == nil {
		session.DisableConnectionObservationAdmission(ctx)
		return ctx, nil
	}
	// Admission owns the generation lock through registration. A concurrent
	// setter either drains this handle or rejects this admission; none escape.
	generation.mu.Lock()
	defer generation.mu.Unlock()
	if generation.observer == nil {
		session.DisableConnectionObservationAdmission(ctx)
		return ctx, nil
	}
	metadata := session.ConnectionMetadata{Network: destination.Network, Destination: destination}
	if inbound := session.InboundFromContext(ctx); inbound != nil {
		metadata.Source, metadata.InboundTag = inbound.Source, inbound.Tag
		if inbound.User != nil {
			metadata.User = inbound.User.Email
		}
	}
	observation := generation.observer.Open(metadata)
	if observation == nil {
		session.DisableConnectionObservationAdmission(ctx)
		return ctx, nil
	}
	handle := session.NewConnectionObservationHandle(observation, generation.finished)
	handle.BindContext(ctx)
	generation.handles[handle] = struct{}{}
	return session.ContextWithConnectionObservation(ctx, handle), handle
}

func observeDestinations(observation *session.ConnectionObservationHandle, network net.Network) func(buf.MultiBuffer) {
	if network != net.Network_UDP {
		return nil
	}
	// Each boundary has a single reader/writer (pipe hooks run under the pipe's
	// existing lock). Suppress repeated destinations without a traffic mutex.
	var last net.Destination
	return func(mb buf.MultiBuffer) {
		if !observation.Active() {
			// Only this boundary accesses last. An idle disabled flow may keep
			// one endpoint until its next I/O; never retain the ledger itself.
			last = net.Destination{}
			return
		}
		for _, b := range mb {
			if b != nil && b.UDP != nil && *b.UDP != last {
				last = *b.UDP
				observation.Destination(last)
			}
		}
	}
}

func observeUpload(observation *session.ConnectionObservationHandle, network net.Network) func(buf.MultiBuffer) {
	destinations := observeDestinations(observation, network)
	return func(mb buf.MultiBuffer) {
		if destinations != nil {
			destinations(mb)
		}
		if !observation.Active() {
			return
		}
		observation.AddUpload(int64(mb.Len()))
	}
}

func observeDownload(observation *session.ConnectionObservationHandle, network net.Network) func(buf.MultiBuffer) {
	destinations := observeDestinations(observation, network)
	return func(mb buf.MultiBuffer) {
		if destinations != nil {
			destinations(mb)
		}
		if !observation.Active() {
			return
		}
		observation.AddDownload(int64(mb.Len()))
	}
}

func observeLink(link *transport.Link, observation *session.ConnectionObservationHandle, network net.Network) {
	// WrapLink provides timeout reads. Hook the original pipe where possible;
	// otherwise count at the wrapper, before cached sniff data is replayed.
	reader := link.Reader.(*buf.TimeoutWrapperReader)
	if pipeReader, ok := reader.Reader.(*pipe.Reader); ok {
		pipeReader.SetReadObserver(observeUpload(observation, network))
	} else {
		reader.Observe = observeUpload(observation, network)
	}
	link.Writer = observeLinkWriter(link.Writer, observation, network)
}

func observeLinkWriter(writer buf.Writer, observation *session.ConnectionObservationHandle, network net.Network) buf.Writer {
	switch writer := writer.(type) {
	case *pipe.Writer:
		writer.SetWriteObserver(observeDownload(observation, network))
		return writer
	case *SizeStatWriter:
		writer.Writer = observeLinkWriter(writer.Writer, observation, network)
		return writer
	default:
		return buf.ObserveWriter(writer, observation.AddDownload, observeDestinations(observation, network), observation.MarkBytesIncomplete)
	}
}

func observeRoute(ctx context.Context, tag string, target, destination net.Destination) {
	if observation := session.ConnectionObservationFromContext(ctx); observation != nil {
		domain := ""
		if destination.Address.Family().IsDomain() {
			domain = destination.Address.Domain()
		}
		observation.Routed(session.ConnectionRoute{OutboundTag: tag, Target: target, Domain: domain})
	}
}

func finishConnection(ctx context.Context, handle *session.ConnectionObservationHandle) {
	if handle == nil {
		return
	}
	state := "closed"
	if ctx.Err() != nil {
		state = "interrupted"
	}
	handle.Finish(state)
}
