package sequencefile

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"time"
)

// A Writer wrapper that:
// - Stores any error that occurs, and stops writing.
// - Keeps track of how many bytes have been written.
// - Closes the wrapped writer, if it's close-able.
type writerWriter struct {
	w     io.Writer
	bytes int
	err   error
}

func (w *writerWriter) setErr(err error) {
	if w.err == nil {
		w.err = err
	}
}

func (w *writerWriter) Write(buf []byte) (n int, err error) {
	if w.err == nil {
		n, w.err = w.w.Write(buf)
		w.bytes += n
	}
	return n, w.err
}

func (w *writerWriter) Close() error {
	cw, ok := w.w.(io.WriteCloser)
	if ok {
		return cw.Close()
	}
	return nil
}

// We don't really need streaming compression.
type compressor interface {
	compress([]byte) ([]byte, error)
}

type gzipCompressor struct {
	gz  *gzip.Writer
	buf bytes.Buffer
}

func (g *gzipCompressor) compress(src []byte) ([]byte, error) {
	if g.gz != nil {
		g.buf.Reset()
		g.gz.Reset(&g.buf)
	} else {
		g.gz = gzip.NewWriter(&g.buf)
	}
	if _, err := g.gz.Write(src); err != nil {
		return nil, err
	}
	if err := g.gz.Close(); err != nil {
		return nil, err
	}
	return g.buf.Bytes(), nil
}

// A WriterConfig specifies the configuration for a Writer.
type WriterConfig struct {
	// Writer is where data will be written to.
	Writer io.Writer

	// KeyClass is the type of each key to be written.
	KeyClass string

	// ValueClass is the type of each value to be written.
	ValueClass string

	// Compression is the type of compression to be used.
	// Either none, record or block.
	Compression Compression

	// CompressionCoded is the codec to be used for compression.
	// This is only relevant if compression is used.
	CompressionCodec CompressionCodec

	// BlockSize is the size of each block for compression.
	// This is only relevant if block compression is used.
	BlockSize int

	// Metadata contains key/value pairs to be added to the header.
	Metadata map[string]string

	// Rand is a source of random numbers. Should usually be nil, but useful
	// for reproducible output.
	Rand *rand.Rand
}

// A Writer writes key/value pairs to an output stream.
type Writer struct {
	cfg         *WriterConfig
	w           *writerWriter
	keyWriter   WritableWriter
	valueWriter WritableWriter
	sync        [SyncSize]byte
	compressor  compressor
}

const (
	seqMagic        = "SEQ"
	seqVersion byte = 6

	syncMarker = -1
	syncBytes  = 2000
)

func NewWriter(cfg *WriterConfig) (w *Writer, err error) {
	// Set some defaults.
	if cfg.KeyClass == "" {
		cfg.KeyClass = BytesWritableClassName
	}
	if cfg.ValueClass == "" {
		cfg.ValueClass = BytesWritableClassName
	}
	if cfg.Compression == 0 {
		cfg.Compression = NoCompression
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	if cfg.Compression == BlockCompression {
		panic("TODO: Block compression")
	}

	keyWriter, err := NewWritableWriter(cfg.KeyClass)
	if err != nil {
		return nil, err
	}
	valueWriter, err := NewWritableWriter(cfg.ValueClass)
	if err != nil {
		return nil, err
	}

	w = &Writer{
		cfg:         cfg,
		w:           &writerWriter{w: cfg.Writer},
		keyWriter:   keyWriter,
		valueWriter: valueWriter,
	}

	if w.cfg.Compression != NoCompression {
		if w.compressor, err = w.newCompressor(w.cfg.CompressionCodec); err != nil {
			return nil, err
		}
	}
	if err := w.writeHeader(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Writer) write(buf []byte) error {
	w.w.Write(buf)
	return w.w.err
}

func (w *Writer) writeString(s string) error {
	WriteVInt(w.w, int64(len(s)))
	return w.write([]byte(s))
}

func (w *Writer) writeBool(b bool) error {
	if b {
		return w.write([]byte{1})
	} else {
		return w.write([]byte{0})
	}
}

func (w *Writer) writeInt32(i int32) error {
	var bs [4]byte
	binary.BigEndian.PutUint32(bs[:], uint32(i))
	return w.write(bs[:])
}

func (w *Writer) writeMetadata() error {
	w.writeInt32(int32(len(w.cfg.Metadata)))
	for k, v := range w.cfg.Metadata {
		w.writeString(k)
		w.writeString(v)
	}
	return w.w.err
}

func (w *Writer) writeHeader() error {
	w.write([]byte(seqMagic))
	w.write([]byte{seqVersion})
	w.writeString(w.cfg.KeyClass)
	w.writeString(w.cfg.ValueClass)
	w.writeBool(w.cfg.Compression != NoCompression)
	w.writeBool(w.cfg.Compression == BlockCompression)

	if w.cfg.Compression != NoCompression {
		codecName, err := w.codecName(w.cfg.CompressionCodec)
		if err != nil {
			return err
		}
		w.writeString(codecName)
	}

	w.writeMetadata()

	w.cfg.Rand.Read(w.sync[:])
	w.write(w.sync[:])

	w.w.bytes = 0
	return w.w.err
}

func (w *Writer) writeSync() error {
	w.writeInt32(syncMarker)
	w.write(w.sync[:])
	w.w.bytes = 0
	return w.w.err
}

func (w *Writer) checkSync() error {
	if w.w.bytes > syncBytes {
		w.writeSync()
	}
	return w.w.err
}

func (w *Writer) doAppend(key []byte, value []byte) error {
	w.checkSync()
	w.writeInt32(int32(len(key) + len(value)))
	w.writeInt32(int32(len(key)))
	w.write(key)
	w.write(value)
	return w.w.err
}

func (w *Writer) Append(key interface{}, value interface{}) (err error) {
	// These errors do not cause the whole writer to error.
	var kbuf, vbuf bytes.Buffer
	if err = w.keyWriter(&kbuf, key); err != nil {
		return
	}
	if err = w.valueWriter(&vbuf, value); err != nil {
		return
	}
	vbytes := vbuf.Bytes()
	if w.cfg.Compression == RecordCompression {
		if vbytes, err = w.compressor.compress(vbytes); err != nil {
			return
		}
	}

	return w.doAppend(kbuf.Bytes(), vbytes)
}

func (w *Writer) Close() (err error) {
	if err = w.w.Close(); err != nil {
		return err
	}
	return w.w.err
}

func (w *Writer) codecName(codec CompressionCodec) (string, error) {
	switch codec {
	case GzipCompression:
		return "org.apache.hadoop.io.compress.GzipCodec", nil
	case SnappyCompression:
		return "org.apache.hadoop.io.compress.SnappyCodec", nil
	default:
		return "", fmt.Errorf("Unknown compression codec: %d", codec)
	}
}

func (w *Writer) newCompressor(codec CompressionCodec) (compressor, error) {
	switch w.cfg.CompressionCodec {
	case GzipCompression:
		return &gzipCompressor{}, nil
	default:
		// TODO: Snappy
		return nil, fmt.Errorf("Unknown compression codec %d", w.cfg.CompressionCodec)
	}
}
