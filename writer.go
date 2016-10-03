package sequencefile

import (
	"bytes"
	"compress/gzip"
	"crypto/rand"
	"encoding/binary"
	"io"

	"github.com/golang/snappy"
)

type Writer struct {
	Header        Header
	writer        io.Writer
	sinceLastSync int
	compressor    compressor
	blockWriter   blockWriter
}

func NewWriter(w io.Writer) *Writer {
	header := Header{
		Version:        SequenceFileVersion,
		KeyClassName:   BytesWritableClassName,
		ValueClassName: BytesWritableClassName,
		Compression:    NoCompression,
	}
	return &Writer{writer: w, Header: header}
}

func (w *Writer) writeSyncMarker() (int, error) {
	if w.Header.SyncMarker == nil {
		syncMarkerBytes := make([]byte, SyncSize)
		_, err := rand.Read(syncMarkerBytes)
		if err != nil {
			return 0, err
		}
		w.Header.SyncMarker = syncMarkerBytes
	}

	w.sinceLastSync = 0
	return w.writer.Write(w.Header.SyncMarker)
}

func (w *Writer) sync() (int, error) {
	if w.Header.Compression == BlockCompression || w.sinceLastSync > 1024*8 {
		totalWritten := 0
		written, err := w.writer.Write([]byte{0xff, 0xff, 0xff, 0xff})
		totalWritten += written
		if err != nil {
			return totalWritten, err
		}
		written, err = w.writeSyncMarker()
		totalWritten += written

		return totalWritten, err
	}
	return 0, nil
}

func (w *Writer) Flush() error {
	if w.Header.Compression == BlockCompression {
		written, err := w.blockWriter.FlushBlock(w)
		w.sinceLastSync += written
		return err
	}

	// we need a bufio.Writer underneath this first, though
	// return w.writer.Flush()
	return nil
}

func (w *Writer) Append(key []byte, value []byte) error {
	totalWritten := 0
	var written int
	var err error

	if w.Header.Compression == BlockCompression {
		w.blockWriter.Append(key, value)
		if w.blockWriter.keysLength+w.blockWriter.valuesLength > BlockCompressionBlockSize {
			return w.Flush()
		}
		return nil
	}

	_, err = w.sync()
	if err != nil {
		return err
	}

	keyLength := len(key)
	keyLengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keyLengthBytes, uint32(keyLength))

	if w.Header.Compression == RecordCompression {
		value, err = w.compress(value)
		if err != nil {
			return err
		}
	}

	recordlength := keyLength + len(value)
	recordLengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordLengthBytes, uint32(recordlength))

	written, err = w.writer.Write(recordLengthBytes)
	totalWritten += written
	if err != nil {
		return err
	}

	written, err = w.writer.Write(keyLengthBytes)
	totalWritten += written
	if err != nil {
		return err
	}

	written, err = w.writer.Write(key)
	totalWritten += written
	if err != nil {
		return err
	}

	written, err = w.writer.Write(value)
	totalWritten += written
	if err != nil {
		return err
	}

	w.sinceLastSync += totalWritten
	return nil
}

func (w *Writer) compress(raw []byte) ([]byte, error) {
	switch w.Header.CompressionCodec {
	case GzipCompression:
		return w.compressGzip(raw)
	case SnappyCompression:
		return w.compressSnappy(raw)
	default:
		panic("compression set without codec")
	}
}

func (w *Writer) compressSnappy(raw []byte) ([]byte, error) {
	rawlen := len(raw)
	rawlenbytes := make([]byte, 4)
	binary.BigEndian.PutUint32(rawlenbytes, uint32(rawlen))

	ret := make([]byte, 0, 4)
	ret = append(ret, rawlenbytes...)

	var chunk []byte
	for offset := 0; offset < rawlen; offset += SnappyBlockSize {
		if offset+SnappyBlockSize > rawlen {
			chunk = raw[offset:]
		} else {
			chunk = raw[offset : offset+SnappyBlockSize]
		}

		tmp := make([]byte, snappy.MaxEncodedLen(len(chunk))+4)
		encoded_chunk := snappy.Encode(tmp, chunk)

		encoded_chunklen := len(encoded_chunk)
		encoded_chunklenbytes := make([]byte, 4)
		binary.BigEndian.PutUint32(encoded_chunklenbytes, uint32(encoded_chunklen))

		ret = append(ret, encoded_chunklenbytes...)
		ret = append(ret, encoded_chunk...)
	}

	return ret, nil
}

func (w *Writer) compressGzip(raw []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	compressor := gzip.NewWriter(buf)

	_, err := compressor.Write(raw)
	if err != nil {
		return nil, err
	}

	err = compressor.Close()
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
