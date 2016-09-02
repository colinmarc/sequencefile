package sequencefile

import (
	"crypto/rand"
	"encoding/binary"
	"io"
)

type Writer struct {
	Header        Header
	writer        io.Writer
	sinceLastSync int
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
