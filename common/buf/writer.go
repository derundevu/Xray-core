package buf

import (
	"io"
	"net"
	"sync"
	"sync/atomic"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/features/stats"
)

// BufferToBytesWriter is a Writer that writes alloc.Buffer into underlying writer.
type BufferToBytesWriter struct {
	io.Writer

	counter         stats.Counter
	cache           [][]byte
	observe         func(int64)
	observeMetadata func(MultiBuffer)
}

// WriteMultiBuffer implements Writer. This method takes ownership of the given buffer.
func (w *BufferToBytesWriter) WriteMultiBuffer(mb MultiBuffer) error {
	if w.observeMetadata != nil {
		w.observeMetadata(mb)
	}
	defer ReleaseMulti(mb)

	size := mb.Len()
	if size == 0 {
		return nil
	}

	if len(mb) == 1 {
		return writeAllBytes(w.Writer, mb[0].Bytes(), w.counter, w.observe)
	}

	if cap(w.cache) < len(mb) {
		w.cache = make([][]byte, 0, len(mb))
	}

	bs := w.cache
	for _, b := range mb {
		bs = append(bs, b.Bytes())
	}

	defer func() {
		for idx := range bs {
			bs[idx] = nil
		}
	}()

	nb := net.Buffers(bs)
	wc := int64(0)
	defer func() {
		if w.counter != nil {
			w.counter.Add(wc)
		}
		if w.observe != nil {
			w.observe(wc)
		}
	}()
	for size > 0 {
		n, err := nb.WriteTo(w.Writer)
		wc += n
		if err != nil {
			return err
		}
		size -= int32(n)
	}

	return nil
}

func (w *BufferToBytesWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if w.observe != nil {
		w.observe(int64(n))
	}
	return n, err
}

// ReadFrom implements io.ReaderFrom.
func (w *BufferToBytesWriter) ReadFrom(reader io.Reader) (int64, error) {
	var sc SizeCounter
	err := Copy(NewReader(reader), w, CountSize(&sc))
	return sc.Size, err
}

// BufferedWriter is a Writer with internal buffer.
type BufferedWriter struct {
	sync.Mutex
	writer          Writer
	buffer          *Buffer
	buffered        bool
	flushNext       bool
	observeCount    func(int64)
	observeMetadata func(MultiBuffer)
	observeSkip     atomic.Int64
	observeFailure  func()
}

// NewBufferedWriter creates a new BufferedWriter.
func NewBufferedWriter(writer Writer) *BufferedWriter {
	return &BufferedWriter{
		writer:   writer,
		buffer:   New(),
		buffered: true,
	}
}

// WriteByte implements io.ByteWriter.
func (w *BufferedWriter) WriteByte(c byte) error {
	return common.Error2(w.Write([]byte{c}))
}

// Write implements io.Writer.
func (w *BufferedWriter) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	w.Lock()
	defer w.Unlock()

	if !w.buffered {
		if writer, ok := w.writer.(io.Writer); ok {
			return writer.Write(b)
		}
	}

	totalBytes := 0
	for len(b) > 0 {
		if w.buffer == nil {
			w.buffer = New()
		}

		nBytes, err := w.buffer.Write(b)
		totalBytes += nBytes

		// ErrBufferFull means a partial write, so flush below and continue
		if err != nil && err != ErrBufferFull {
			return totalBytes, err
		}
		if !w.buffered || w.buffer.IsFull() {
			if err := w.flushInternal(); err != nil {
				return totalBytes, err
			}
		}
		b = b[nBytes:]
	}

	return totalBytes, nil
}

// WriteMultiBuffer implements Writer. It takes ownership of the given MultiBuffer.
func (w *BufferedWriter) WriteMultiBuffer(b MultiBuffer) error {
	if b.IsEmpty() {
		return nil
	}

	w.Lock()
	defer w.Unlock()
	if w.observeMetadata != nil {
		w.observeMetadata(b)
	}

	if !w.buffered {
		return w.writer.WriteMultiBuffer(b)
	}

	reader := MultiBufferContainer{
		MultiBuffer: b,
	}
	defer reader.Close()

	for !reader.MultiBuffer.IsEmpty() {
		if w.buffer == nil {
			w.buffer = New()
		}
		common.Must2(w.buffer.ReadFrom(&reader))
		if w.buffer.IsFull() {
			if err := w.flushInternal(); err != nil {
				return err
			}
		}
	}

	if w.flushNext {
		w.buffered = false
		w.flushNext = false
		return w.flushInternal()
	}

	return nil
}

// Flush flushes buffered content into underlying writer.
func (w *BufferedWriter) Flush() error {
	w.Lock()
	defer w.Unlock()

	return w.flushInternal()
}

func (w *BufferedWriter) flushInternal() (flushErr error) {
	if w.buffer.IsEmpty() {
		return nil
	}

	b := w.buffer
	w.buffer = nil
	if w.observeCount != nil {
		// Failed flushes relinquish their buffer, including any unwritten
		// pre-analytics prefix. Successful nested buffering retains that prefix
		// until the underlying writer reports its actual delivered bytes.
		defer func() {
			if flushErr != nil {
				w.observedWriteFailed()
			}
		}()
	}

	if writer, ok := w.writer.(io.Writer); ok {
		err := WriteAllBytes(writer, b.Bytes(), nil)
		b.Release()
		return err
	}

	return w.writer.WriteMultiBuffer(MultiBuffer{b})
}

func (w *BufferedWriter) observeDelivered(n int64) {
	for {
		skip := w.observeSkip.Load()
		take := min(n, skip)
		if take == 0 || w.observeSkip.CompareAndSwap(skip, skip-take) {
			if n > take {
				w.observeCount(n - take)
			}
			return
		}
	}
}

func (w *BufferedWriter) observedWriteFailed() {
	w.observeSkip.Store(0)
	if w.observeFailure != nil {
		w.observeFailure()
	}
}

// SetBuffered sets whether the internal buffer is used. If set to false, Flush() will be called to clear the buffer.
func (w *BufferedWriter) SetBuffered(f bool) error {
	w.Lock()
	defer w.Unlock()

	w.buffered = f
	if !f {
		return w.flushInternal()
	}
	return nil
}

// SetFlushNext will wait the next WriteMultiBuffer to flush and set buffered = false
func (w *BufferedWriter) SetFlushNext() {
	w.Lock()
	defer w.Unlock()
	w.flushNext = true
}

// ReadFrom implements io.ReaderFrom.
func (w *BufferedWriter) ReadFrom(reader io.Reader) (int64, error) {
	if err := w.SetBuffered(false); err != nil {
		return 0, err
	}

	var sc SizeCounter
	err := Copy(NewReader(reader), w, CountSize(&sc))
	return sc.Size, err
}

// Close implements io.Closable.
func (w *BufferedWriter) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}
	return common.Close(w.writer)
}

// SequentialWriter is a Writer that writes MultiBuffer sequentially into the underlying io.Writer.
type SequentialWriter struct {
	io.Writer
	observe         func(int64)
	observeMetadata func(MultiBuffer)
}

// WriteMultiBuffer implements Writer.
func (w *SequentialWriter) WriteMultiBuffer(mb MultiBuffer) error {
	if w.observeMetadata != nil {
		w.observeMetadata(mb)
	}
	writer := w.Writer
	if w.observe != nil {
		writer = w
	}
	mb, err := WriteMultiBuffer(writer, mb)
	ReleaseMulti(mb)
	return err
}

func (w *SequentialWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if w.observe != nil {
		w.observe(int64(n))
	}
	return n, err
}

type noOpWriter byte

func (noOpWriter) WriteMultiBuffer(b MultiBuffer) error {
	ReleaseMulti(b)
	return nil
}

func (noOpWriter) Write(b []byte) (int, error) {
	return len(b), nil
}

func (noOpWriter) ReadFrom(reader io.Reader) (int64, error) {
	b := New()
	defer b.Release()

	totalBytes := int64(0)
	for {
		b.Clear()
		_, err := b.ReadFrom(reader)
		totalBytes += int64(b.Len())
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return totalBytes, nil
			}
			return totalBytes, err
		}
	}
}

var (
	// Discard is a Writer that swallows all contents written in.
	Discard Writer = noOpWriter(0)

	// DiscardBytes is an io.Writer that swallows all contents written in.
	DiscardBytes io.Writer = noOpWriter(0)
)
