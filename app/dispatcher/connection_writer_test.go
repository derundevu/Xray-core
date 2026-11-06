package dispatcher_test

import (
	"io"
	"testing"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/transport/pipe"
)

type partialConnectionWriter struct{}

func (partialConnectionWriter) Write(p []byte) (int, error) { return min(3, len(p)), io.ErrClosedPipe }

func TestConnectionBufferedStatisticsWriterPreservesKnownPrefix(t *testing.T) {
	counter := new(TestCounter)
	statistics := &dispatcher.SizeStatWriter{Counter: counter, Writer: &buf.BufferToBytesWriter{Writer: partialConnectionWriter{}}}
	buffered := buf.NewBufferedWriter(statistics)
	var total int64
	incomplete := false
	observed := buf.ObserveWriter(buffered, func(n int64) { total += n }, nil, func() { incomplete = true })
	if observed != buffered {
		t.Fatal("buffered writer replaced")
	}
	if err := observed.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("payload"))}); err != nil {
		t.Fatal(err)
	}
	if err := buffered.Flush(); err == nil {
		t.Fatal("expected partial flush failure")
	}
	if total != 3 || incomplete {
		t.Fatalf("statistics wrapper lost known prefix: %d, incomplete %v", total, incomplete)
	}
}

func TestConnectionBufferedStatisticsPipePreservesAtomicFailure(t *testing.T) {
	reader, writer := pipe.New()
	defer reader.Interrupt()
	counter := new(TestCounter)
	statistics := &dispatcher.SizeStatWriter{Counter: counter, Writer: writer}
	buffered := buf.NewBufferedWriter(statistics)
	var total int64
	incomplete := false
	buf.ObserveWriter(buffered, func(n int64) { total += n }, nil, func() { incomplete = true })
	if err := buffered.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("kept"))}); err != nil {
		t.Fatal(err)
	}
	if err := buffered.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := buffered.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes([]byte("failed"))}); err != nil {
		t.Fatal(err)
	}
	if err := buffered.Flush(); err == nil {
		t.Fatal("expected atomic closed pipe failure")
	}
	if total != 4 || incomplete {
		t.Fatal("atomic pipe failure discarded known bytes or advertised uncertainty")
	}
}
