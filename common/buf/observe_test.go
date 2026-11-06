package buf_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
)

type partialObservedWriter struct{}

func (partialObservedWriter) Write(p []byte) (int, error) { return min(3, len(p)), io.ErrClosedPipe }

type observedFromWriter struct{}

func (observedFromWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	buf.ReleaseMulti(mb)
	return nil
}
func (observedFromWriter) ReadFrom(r io.Reader) (int64, error) { return io.Copy(io.Discard, r) }

func TestObservedNativeWriterExactPartialBytes(t *testing.T) {
	for _, vector := range []bool{false, true} {
		var total int64
		writer := &buf.BufferToBytesWriter{Writer: partialObservedWriter{}}
		observed := buf.ObserveWriter(writer, func(n int64) { total += n }, nil, nil)
		if observed != writer {
			t.Fatal("native vector writer was replaced")
		}
		mb := buf.MultiBuffer{buf.FromBytes([]byte("abcdef"))}
		if vector {
			mb = append(mb, buf.FromBytes([]byte("ghi")))
		}
		if err := observed.WriteMultiBuffer(mb); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatal(err)
		}
		if total != 3 {
			t.Fatalf("partial write counted %d bytes, want 3", total)
		}
	}
}
func TestObservedWriterPreservesReaderFrom(t *testing.T) {
	var total int64
	writer := buf.ObserveWriter(observedFromWriter{}, func(n int64) { total += n }, nil, nil)
	if _, ok := writer.(io.Writer); ok {
		t.Fatal("invented unsupported byte writer")
	}
	rf, ok := writer.(io.ReaderFrom)
	if !ok {
		t.Fatal("lost ReaderFrom")
	}
	n, err := rf.ReadFrom(bytes.NewBufferString("payload"))
	if err != nil || n != 7 || total != n {
		t.Fatalf("ReadFrom = %d, %v; count %d", n, err, total)
	}
}
func TestObservedSequentialWriterExactPartialBytes(t *testing.T) {
	var total int64
	writer := &buf.SequentialWriter{Writer: partialObservedWriter{}}
	observed := buf.ObserveWriter(writer, func(n int64) { total += n }, nil, nil)
	if observed != writer {
		t.Fatal("sequential writer was replaced")
	}
	if err := observed.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("abcdef"))}); err == nil {
		t.Fatal("lost write failure")
	}
	if total != 3 {
		t.Fatalf("partial datagram counted %d bytes", total)
	}
}

type prefixObservedWriter struct {
	remaining int
	delivered bytes.Buffer
}

func (w *prefixObservedWriter) Write(p []byte) (int, error) {
	n := min(w.remaining, len(p))
	w.remaining -= n
	w.delivered.Write(p[:n])
	if n < len(p) {
		return n, io.ErrClosedPipe
	}
	return n, nil
}

func TestObservedBufferedWriterPreservesPartialApplicationPrefix(t *testing.T) {
	for _, prefix := range []string{"", "unobserved header:"} {
		socket := &prefixObservedWriter{remaining: len(prefix) + 3}
		buffered := buf.NewBufferedWriter(&buf.BufferToBytesWriter{Writer: socket})
		if _, err := buffered.Write([]byte(prefix)); err != nil {
			t.Fatal(err)
		}
		var total int64
		incomplete := false
		observed := buf.ObserveWriter(buffered, func(n int64) { total += n }, nil, func() { incomplete = true })
		if observed != buffered {
			t.Fatal("lost BufferedWriter concrete/optional interfaces")
		}
		if err := observed.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("abcdefghij"))}); err != nil {
			t.Fatal(err)
		}
		if total != 0 {
			t.Fatal("credited data still buffered in memory")
		}
		if err := buffered.Flush(); err == nil {
			t.Fatal("expected partial flush failure")
		}
		if total != 3 || incomplete || socket.delivered.String() != prefix+"abc" {
			t.Fatalf("known application prefix = %d, incomplete %v, delivered %q", total, incomplete, socket.delivered.String())
		}
		socket.remaining = 2
		if err := observed.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("xy"))}); err != nil {
			t.Fatal(err)
		}
		if err := buffered.Flush(); err != nil {
			t.Fatal(err)
		}
		if total != 5 || incomplete {
			t.Fatal("discarded prefix contaminated the next flush")
		}
	}
}

func TestObservedNestedBufferedFailureClearsDiscardedPrefix(t *testing.T) {
	socket := &prefixObservedWriter{remaining: 2}
	inner := buf.NewBufferedWriter(&buf.SequentialWriter{Writer: socket})
	outer := buf.NewBufferedWriter(inner)
	if _, err := outer.Write([]byte("header")); err != nil {
		t.Fatal(err)
	}
	var total int64
	incomplete := false
	buf.ObserveWriter(outer, func(n int64) { total += n }, nil, func() { incomplete = true })
	if err := outer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("application"))}); err != nil {
		t.Fatal(err)
	}
	if err := outer.Flush(); err != nil {
		t.Fatal(err)
	}
	if total != 0 {
		t.Fatal("inner buffered data was prematurely credited")
	}
	if err := inner.Flush(); err == nil {
		t.Fatal("expected inner partial flush failure")
	}
	if total != 0 || incomplete {
		t.Fatal("counted discarded application bytes or marked a known prefix inexact")
	}
	socket.remaining = 3
	if err := outer.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("xyz"))}); err != nil {
		t.Fatal(err)
	}
	if err := outer.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := inner.Flush(); err != nil {
		t.Fatal(err)
	}
	if total != 3 || incomplete {
		t.Fatal("nested discarded header consumed later application bytes")
	}
}

func TestObservedEndpointOverrideKeepsMetadataAndPartialBytes(t *testing.T) {
	writer := &buf.EndpointOverrideWriter{
		Writer: &buf.SequentialWriter{Writer: partialObservedWriter{}},
		Dest:   net.LocalHostIP, OriginalDest: net.LocalHostIPv6,
	}
	var total int64
	incomplete := false
	var destination net.Destination
	observed := buf.ObserveWriter(writer, func(n int64) { total += n }, func(mb buf.MultiBuffer) { destination = *mb[0].UDP }, func() { incomplete = true })
	if observed != writer {
		t.Fatal("endpoint wrapper was replaced")
	}
	packet := buf.FromBytes([]byte("abcdef"))
	packet.UDP = &net.Destination{Network: net.Network_UDP, Address: net.LocalHostIP, Port: 53}
	if err := observed.WriteMultiBuffer(buf.MultiBuffer{packet}); err == nil {
		t.Fatal("expected partial write failure")
	}
	if total != 3 || incomplete || destination.Address != net.LocalHostIPv6 {
		t.Fatal("lost known prefix or endpoint override metadata")
	}
}

type opaqueObservedWriter struct {
	fail      bool
	delivered int64
}

func (w *opaqueObservedWriter) WriteMultiBuffer(mb buf.MultiBuffer) error {
	defer buf.ReleaseMulti(mb)
	if w.fail {
		w.delivered += 2
		return io.ErrClosedPipe
	}
	w.delivered += int64(mb.Len())
	return nil
}
func TestObservedOpaqueFailurePermanentlyMarksUnavailablePrefix(t *testing.T) {
	writer := new(opaqueObservedWriter)
	var total int64
	incomplete := false
	observed := buf.ObserveWriter(writer, func(n int64) { total += n }, nil, func() { incomplete = true })
	if err := observed.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("known"))}); err != nil {
		t.Fatal(err)
	}
	writer.fail = true
	if err := observed.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("unknown partial"))}); err == nil {
		t.Fatal("expected opaque failure")
	}
	if total != 5 || !incomplete || writer.delivered != 7 {
		t.Fatal("opaque failure lost confirmed bytes or advertised exactness")
	}
}
