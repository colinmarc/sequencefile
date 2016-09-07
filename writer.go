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
}

func NewWriter(w io.Writer) *Writer {
	header := Header{
		Version:        6,
		KeyClassName:   "org.apache.hadoop.io.BytesWritable",
		ValueClassName: "org.apache.hadoop.io.BytesWritable",
		Compression:    NoCompression,
	}
	return &Writer{writer: w, Header: header, sinceLastSync: 0}
}

func (w *Writer) Write(bytes []byte) (int, error) {
	return w.writer.Write(bytes)
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

// maybe
func (w *Writer) sync() (int, error) {
	if w.sinceLastSync > 1024 {
		return w.writeSyncMarker()
	}
	return 0, nil
}

func (w *Writer) Append(key []byte, value []byte) (int, error) {
	// TODO: if we haven't written the header yet, should we error, or just silently write the header?
	var written int
	var err error
	totalwritten := 0

	written, err = w.sync()
	totalwritten += written
	if err != nil {
		return totalwritten, err
	}

	keylength := len(key)
	keylengthbytes := make([]byte, 4)
	binary.BigEndian.PutUint32(keylengthbytes, uint32(keylength))

	if w.Header.Compression == RecordCompression {
		value, err = w.compress(value)
		if err != nil {
			return totalwritten, err
		}
	}

	recordlength := keylength + len(value)
	recordlengthbytes := make([]byte, 4)
	binary.BigEndian.PutUint32(recordlengthbytes, uint32(recordlength))

	written, err = w.writer.Write(recordlengthbytes)
	totalwritten += written
	if err != nil {
		return totalwritten, err
	}

	written, err = w.writer.Write(keylengthbytes)
	totalwritten += written
	if err != nil {
		return totalwritten, err
	}

	written, err = w.writer.Write(key)
	totalwritten += written
	if err != nil {
		return totalwritten, err
	}

	written, err = w.writer.Write(value)
	totalwritten += written
	if err != nil {
		return totalwritten, err
	}

	return totalwritten, nil
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
