package sequencefile

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"math/rand"
	"time"
)

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

// A Writer writes key/value pairs to a sequence file output stream.
type Writer struct {
	cfg         *WriterConfig
	w           *writerHelper
	keyWriter   WritableWriter
	valueWriter WritableWriter
	sync        [SyncSize]byte
	pairs       pairWriter
	compressor  compressor
}

const (
	seqMagic        = "SEQ"
	seqVersion byte = 6
)

// NewWriter constructs a new Writer.
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
		w:           &writerHelper{w: cfg.Writer},
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

	if err := w.initPairWriter(); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *Writer) writeMetadata() error {
	w.w.writeInt32(int32(len(w.cfg.Metadata)))
	for k, v := range w.cfg.Metadata {
		w.w.writeString(k)
		w.w.writeString(v)
	}
	return w.w.err
}

func (w *Writer) writeHeader() error {
	w.w.write([]byte(seqMagic))
	w.w.write([]byte{seqVersion})
	w.w.writeString(w.cfg.KeyClass)
	w.w.writeString(w.cfg.ValueClass)
	w.w.writeBool(w.cfg.Compression != NoCompression)
	w.w.writeBool(w.cfg.Compression == BlockCompression)

	if w.cfg.Compression != NoCompression {
		codecName, err := w.codecName(w.cfg.CompressionCodec)
		if err != nil {
			return err
		}
		w.w.writeString(codecName)
	}

	w.writeMetadata()

	w.cfg.Rand.Read(w.sync[:])
	w.w.write(w.sync[:])

	w.w.bytes = 0
	return w.w.err
}

// Append adds a key/value pair to this Writer.
// The types of the key and value must match the KeyClass and ValueClass
// this Writer was configured with.
func (w *Writer) Append(key interface{}, value interface{}) (err error) {
	// These errors do not cause the whole writer to error.
	var kbuf, vbuf bytes.Buffer
	if err = w.keyWriter(&kbuf, key); err != nil {
		return
	}
	if err = w.valueWriter(&vbuf, value); err != nil {
		return
	}
	return w.pairs.Write(kbuf.Bytes(), vbuf.Bytes())
}

// AppendBuffered appends a key/value pair but allows the caller to re-use buffers to avoid heap allocations.
func (w *Writer) AppendBuffered(key, value interface{}, kbuf, vbuf bytes.Buffer) (err error) {
	// These errors do not cause the whole writer to error.
	kbuf.Reset()
	if err = w.keyWriter(&kbuf, key); err != nil {
		return
	}
	vbuf.Reset()
	if err = w.valueWriter(&vbuf, value); err != nil {
		return
	}
	return w.pairs.Write(kbuf.Bytes(), vbuf.Bytes())
}

// Close frees resources held by this Writer.
func (w *Writer) Close() error {
	var ret error
	if err := w.pairs.Close(); err != nil {
		ret = err
	}
	if err := w.w.Close(); err != nil {
		ret = err
	}
	if w.w.err != nil {
		ret = w.w.err
	}
	return ret
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
	case SnappyCompression:
		return snappyCompressor{snappyDefaultChunkSize}, nil
	default:
		return nil, fmt.Errorf("Unknown compression codec %d", w.cfg.CompressionCodec)
	}
}

func (w *Writer) initPairWriter() error {
	switch w.cfg.Compression {
	case NoCompression:
		w.pairs = &uncompressedPairs{w.w, w.sync[:]}
	case RecordCompression:
		w.pairs = &recordCompressedPairs{uncompressedPairs{w.w, w.sync[:]}, w.compressor}
	case BlockCompression:
		w.pairs = &blockPairs{
			w:          w.w,
			sync:       w.sync[:],
			compressor: w.compressor,
			blockSize:  w.cfg.BlockSize,
		}
	}
	if w.pairs == nil {
		return fmt.Errorf("Unknown compression type %d", w.cfg.Compression)
	}
	return w.pairs.Init()
}
