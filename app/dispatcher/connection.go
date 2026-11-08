package dispatcher

import (
	"context"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

// ConnectionObserver belongs to one dispatcher instance. Install before Start.
type ConnectionObserver interface {
	Open(session.ConnectionMetadata) session.ConnectionObservation
}

func (d *DefaultDispatcher) SetConnectionObserver(observer ConnectionObserver) {
	d.connectionObserver = observer
}

func (d *DefaultDispatcher) openConnection(ctx context.Context, destination net.Destination) (context.Context, *session.ConnectionObservationHandle) {
	if d.connectionObserver == nil || session.ConnectionObservationDisabledFromContext(ctx) || session.ConnectionObservationFromContext(ctx) != nil {
		return ctx, nil
	}
	metadata := session.ConnectionMetadata{Network: destination.Network, Destination: destination}
	if inbound := session.InboundFromContext(ctx); inbound != nil {
		metadata.Source, metadata.InboundTag = inbound.Source, inbound.Tag
		if inbound.User != nil {
			metadata.User = inbound.User.Email
		}
	}
	observation := d.connectionObserver.Open(metadata)
	if observation == nil {
		return session.ContextWithConnectionObservationDisabled(ctx), nil
	}
	handle := session.NewConnectionObservationHandle(observation)
	handle.BindContext(ctx)
	return session.ContextWithConnectionObservation(ctx, handle), handle
}

func observeDestinations(observation session.ConnectionObservation, network net.Network) func(buf.MultiBuffer) {
	if network != net.Network_UDP {
		return nil
	}
	// Each boundary has a single reader/writer (pipe hooks run under the pipe's
	// existing lock). Suppress repeated destinations without a traffic mutex.
	var last net.Destination
	return func(mb buf.MultiBuffer) {
		for _, b := range mb {
			if b != nil && b.UDP != nil && *b.UDP != last {
				last = *b.UDP
				observation.Destination(last)
			}
		}
	}
}

func observeUpload(observation session.ConnectionObservation, network net.Network) func(buf.MultiBuffer) {
	destinations := observeDestinations(observation, network)
	return func(mb buf.MultiBuffer) {
		if destinations != nil {
			destinations(mb)
		}
		observation.AddUpload(int64(mb.Len()))
	}
}

func observeDownload(observation session.ConnectionObservation, network net.Network) func(buf.MultiBuffer) {
	destinations := observeDestinations(observation, network)
	return func(mb buf.MultiBuffer) {
		if destinations != nil {
			destinations(mb)
		}
		observation.AddDownload(int64(mb.Len()))
	}
}

func observeLink(link *transport.Link, observation session.ConnectionObservation, network net.Network) {
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

func observeLinkWriter(writer buf.Writer, observation session.ConnectionObservation, network net.Network) buf.Writer {
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
