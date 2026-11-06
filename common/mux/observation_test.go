package mux

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

type muxObservation struct {
	upload, download atomic.Int64
	state            string
	done             chan struct{}
}

func (*muxObservation) Routed(session.ConnectionRoute) {}
func (*muxObservation) Destination(net.Destination)    {}
func (o *muxObservation) AddUpload(n int64)            { o.upload.Add(n) }
func (o *muxObservation) AddDownload(n int64)          { o.download.Add(n) }
func (*muxObservation) SetDownloadDeferred(bool)       {}
func (*muxObservation) MarkBytesIncomplete()           {}
func (o *muxObservation) Finish(state string)          { o.state = state; close(o.done) }

func TestMuxObservationWaitsForConcurrentOutput(t *testing.T) {
	manager := NewSessionManager()
	input, inputWriter := pipe.New()
	outputReader, output := pipe.New()
	defer inputWriter.Close()
	defer outputReader.Interrupt()
	var finished atomic.Int64
	s := manager.Allocate(&ClientStrategy{}, input, output, protocol.TransferTypeStream, func(string) { finished.Add(1) })
	if !s.beginOutput() {
		t.Fatal("output rejected before session closed")
	}
	if err := s.Close(false); err != nil {
		t.Fatal(err)
	}
	if finished.Load() != 0 {
		t.Fatal("finished while an output write was still active")
	}
	s.endOutput()
	if finished.Load() != 1 {
		t.Fatal("did not finish after output drained")
	}
	if s.beginOutput() {
		t.Fatal("accepted output after close")
	}
	s.Close(false)
	if finished.Load() != 1 {
		t.Fatal("finished twice")
	}
}

func TestMuxObservationKeepsConcretePipeLifetimeAndPackets(t *testing.T) {
	downReader, downWriter := pipe.New()
	upReader, upWriter := pipe.New()
	worker, err := NewClientWorker(transport.Link{Reader: downReader, Writer: upWriter}, ClientStrategy{})
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	defer upReader.Interrupt()
	defer downWriter.Close()
	input, inputWriter := pipe.New()
	outputReader, output := pipe.New()
	defer inputWriter.Close()
	defer outputReader.Interrupt()
	observation := &muxObservation{done: make(chan struct{})}
	handle := session.NewConnectionObservationHandle(observation)
	inputWriter.SetWriteObserver(func(mb buf.MultiBuffer) { handle.AddUpload(int64(mb.Len())) })
	output.SetWriteObserver(func(mb buf.MultiBuffer) { handle.AddDownload(int64(mb.Len())) })
	destination := net.UDPDestination(net.LocalHostIP, 53)
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{Target: destination}})
	ctx = session.ContextWithConnectionObservation(ctx, handle)
	if !worker.Dispatch(ctx, &transport.Link{Reader: input, Writer: output}) {
		t.Fatal("dispatch failed")
	}
	handle.Finish("closed")
	select {
	case <-observation.done:
		t.Fatal("mux Dispatch return ended logical flow")
	default:
	}
	if err := inputWriter.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("query"))}); err != nil {
		t.Fatal(err)
	}
	frame, err := upReader.ReadMultiBufferTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	buf.ReleaseMulti(frame)
	remote := NewResponseWriter(1, downWriter, protocol.TransferTypePacket)
	reply := buf.FromBytes([]byte("reply"))
	source := net.UDPDestination(net.LocalHostIP, 54)
	reply.UDP = &source
	if err := remote.WriteMultiBuffer(buf.MultiBuffer{reply}); err != nil {
		t.Fatal(err)
	}
	got, err := outputReader.ReadMultiBufferTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].UDP == nil || *got[0].UDP != source {
		t.Fatal("mux changed per-packet source")
	}
	buf.ReleaseMulti(got)
	select {
	case <-observation.done:
		t.Fatal("reply half ended flow")
	default:
	}
	if err := remote.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-observation.done:
	case <-time.After(5 * time.Second):
		t.Fatal("remote end did not finish logical flow")
	}
	if observation.state != "closed" || observation.upload.Load() != 5 || observation.download.Load() != 5 {
		t.Fatalf("state %s, bytes %d/%d", observation.state, observation.upload.Load(), observation.download.Load())
	}
}
