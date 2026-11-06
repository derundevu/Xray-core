package buf

import (
	"io"

	"github.com/xtls/xray-core/common"
)

// ObserveWriter observes accepted application bytes without retaining buffers.
// Optional byte-writer and ReaderFrom paths are kept, including partial writes.
// Concrete pipe endpoints install hooks instead of using this adapter.
func ObserveWriter(writer Writer, count func(int64), metadata func(MultiBuffer), incomplete func()) Writer {
	return observeWriter(writer, count, metadata, incomplete, nil)
}

func observeWriter(writer Writer, count func(int64), metadata func(MultiBuffer), incomplete, failed func()) Writer {
	// These native writers know exact partial MultiBuffer writes. Keep their
	// concrete shape and vectorized writes rather than wrapping the socket.
	switch w := writer.(type) {
	case *BufferToBytesWriter:
		w.observe = count
		w.observeMetadata = metadata
		return w
	case *SequentialWriter:
		w.observe = count
		w.observeMetadata = metadata
		return w
	case *BufferedWriter:
		w.observeCount = count
		w.observeMetadata = metadata
		w.observeSkip.Store(int64(w.buffer.Len()))
		w.observeFailure = failed
		w.writer = observeWriter(w.writer, w.observeDelivered, nil, incomplete, w.observedWriteFailed)
		return w
	case *EndpointOverrideWriter:
		w.Writer = observeWriter(w.Writer, count, metadata, incomplete, failed)
		return w
	case interface{ ObservationWriter() *Writer }:
		// Transparent wrappers expose their existing writer slot before use;
		// retain their concrete shape and instrument the actual accepting writer.
		inner := w.ObservationWriter()
		*inner = observeWriter(*inner, count, metadata, incomplete, failed)
		return writer
	case interface{ SetWriteObserver(func(MultiBuffer)) }:
		// Concrete pipes report only atomic accepted writes under their own lock.
		w.SetWriteObserver(func(mb MultiBuffer) {
			if metadata != nil {
				metadata(mb)
			}
			count(int64(mb.Len()))
		})
		return writer
	}
	w := &observedWriter{Writer: writer, count: count, metadata: metadata, incomplete: incomplete, failed: failed}
	byteWriter, bytes := writer.(io.Writer)
	readerFrom, from := writer.(io.ReaderFrom)
	switch {
	case bytes && from:
		return &observedWriterBoth{observedWriter: w, byteWriter: byteWriter, readerFrom: readerFrom}
	case bytes:
		return &observedByteWriter{observedWriter: w, byteWriter: byteWriter}
	case from:
		return &observedReaderFrom{observedWriter: w, readerFrom: readerFrom}
	default:
		return w
	}
}

type observedWriter struct {
	Writer
	count      func(int64)
	metadata   func(MultiBuffer)
	incomplete func()
	failed     func()
}

func (w *observedWriter) WriteMultiBuffer(mb MultiBuffer) error {
	if w.metadata != nil {
		w.metadata(mb)
	}
	n := int64(mb.Len())
	err := w.Writer.WriteMultiBuffer(mb)
	if err == nil {
		w.count(n)
	} else if w.incomplete != nil {
		// An opaque Writer has no partial-byte result. Preserve prior confirmed
		// bytes, but never represent this failed write as an exact zero.
		w.incomplete()
	}
	if err != nil && w.failed != nil {
		w.failed()
	}
	return err
}
func (w *observedWriter) Close() error {
	err := common.Close(w.Writer)
	if err != nil && w.incomplete != nil {
		w.incomplete()
	}
	if err != nil && w.failed != nil {
		w.failed()
	}
	return err
}
func (w *observedWriter) Interrupt() { common.Interrupt(w.Writer) }

type observedByteWriter struct {
	*observedWriter
	byteWriter io.Writer
}

func (w *observedByteWriter) Write(p []byte) (int, error) {
	n, err := w.byteWriter.Write(p)
	w.count(int64(n))
	return n, err
}

type observedReaderFrom struct {
	*observedWriter
	readerFrom io.ReaderFrom
}

func (w *observedReaderFrom) ReadFrom(r io.Reader) (int64, error) {
	n, err := w.readerFrom.ReadFrom(r)
	w.count(n)
	return n, err
}

type observedWriterBoth struct {
	*observedWriter
	byteWriter io.Writer
	readerFrom io.ReaderFrom
}

func (w *observedWriterBoth) Write(p []byte) (int, error) {
	n, err := w.byteWriter.Write(p)
	w.count(int64(n))
	return n, err
}
func (w *observedWriterBoth) ReadFrom(r io.Reader) (int64, error) {
	n, err := w.readerFrom.ReadFrom(r)
	w.count(n)
	return n, err
}
