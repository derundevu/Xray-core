package pipe_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/pipe"
)

func TestPipeObservationKeepsPacketsAndErrorFeedback(t *testing.T) {
	r, w := pipe.New()
	var writes, reads int64
	var destinations []net.Destination
	w.SetWriteObserver(func(mb buf.MultiBuffer) {
		writes += int64(mb.Len())
		for _, b := range mb {
			if b.UDP != nil {
				destinations = append(destinations, *b.UDP)
			}
		}
	})
	r.SetReadObserver(func(mb buf.MultiBuffer) { reads += int64(mb.Len()) })
	first := net.UDPDestination(net.LocalHostIP, 53)
	second := net.UDPDestination(net.LocalHostIP, 54)
	mb := buf.MultiBuffer{buf.FromBytes([]byte("one")), buf.FromBytes([]byte("two"))}
	mb[0].UDP, mb[1].UDP = &first, &second
	if err := w.WriteMultiBuffer(mb); err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadMultiBufferTimeout(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.ReleaseMulti(got)
	if len(got) != 2 || got[0].UDP != &first || got[1].UDP != &second {
		t.Fatal("packet metadata changed")
	}
	if writes != 6 || reads != 6 || len(destinations) != 2 {
		t.Fatal("lost bytes or association destination")
	}
	r.ReturnAnError(io.EOF)
	if _, err := r.ReadMultiBuffer(); !errors.Is(err, io.EOF) {
		t.Fatal("lost mux error feedback", err)
	}
	if err := r.Recover(); err != nil {
		t.Fatal("feedback delivered twice", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadMultiBuffer(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

func TestPipeObservationExcludesDiscardedAndFailedWrites(t *testing.T) {
	r, w := pipe.New(pipe.WithSizeLimit(0), pipe.DiscardOverflow())
	defer r.Interrupt()
	var accepted int64
	w.SetWriteObserver(func(mb buf.MultiBuffer) { accepted += int64(mb.Len()) })
	if err := w.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("kept"))}); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("discarded"))}); err != nil {
		t.Fatal(err)
	}
	if accepted != 4 {
		t.Fatalf("counted discarded traffic: %d", accepted)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("failed"))}); err == nil {
		t.Fatal("expected closed pipe")
	}
	if accepted != 4 {
		t.Fatal("counted failed write")
	}
}
