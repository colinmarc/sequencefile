package sequencefile

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sort"
)

// A Header represents the information contained in the header of the
// SequenceFile.
type Header struct {
	Version                   int
	Compression               Compression
	CompressionCodec          CompressionCodec
	CompressionCodecClassName string
	KeyClassName              string
	ValueClassName            string
	Metadata                  map[string]string
	SyncMarker                []byte
}

// ReadHeader parses the SequenceFile header from the input stream, and fills
// in the Header struct with the values. This should be called when the reader
// is positioned at the start of the file or input stream, before any records
// are read.
//
// ReadHeader will also validate that the settings of the SequenceFile
// (version, compression, key/value serialization, etc) are compatible.
func (r *Reader) ReadHeader() error {
	magic, err := r.consume(4)
	if err != nil {
		return fmt.Errorf("sequencefile: reading magic number: %s", err)
	} else if string(magic[:3]) != "SEQ" {
		return fmt.Errorf("sequencefile: invalid magic number: %s", magic)
	}

	r.Header.Version = int(magic[3])
	if r.Header.Version < 5 {
		return fmt.Errorf("sequencefile: unsupported version: %d", r.Header.Version)
	}

	keyClassName, err := r.readString()
	if err != nil {
		return err
	}

	valueClassName, err := r.readString()
	if err != nil {
		return err
	}

	r.Header.KeyClassName = keyClassName
	r.Header.ValueClassName = valueClassName

	valueCompression, err := r.readBoolean()
	if err != nil {
		return err
	}

	blockCompression, err := r.readBoolean()
	if err != nil {
		return err
	}

	if blockCompression {
		r.Header.Compression = BlockCompression
	} else if valueCompression {
		r.Header.Compression = RecordCompression
	} else {
		r.Header.Compression = NoCompression
	}

	if r.Header.Compression != NoCompression {
		compressionCodecClassName, err := r.readString()
		if err != nil {
			return err
		}

		// TODO: DRY this out along with other COMPRESSION
		r.Header.CompressionCodecClassName = compressionCodecClassName
		switch r.Header.CompressionCodecClassName {
		case GzipClassName:
			r.Header.CompressionCodec = GzipCompression
		case SnappyClassName:
			r.Header.CompressionCodec = SnappyCompression
		default:
			return fmt.Errorf("sequencefile: unsupported compression codec: %s", r.Header.CompressionCodecClassName)
		}
	}

	r.compression = r.Header.Compression
	r.codec = r.Header.CompressionCodec

	err = r.readMetadata()
	if err != nil {
		return err
	}

	r.clear()
	marker, err := r.consume(SyncSize)
	if err != nil {
		return err
	}

	r.Header.SyncMarker = marker
	r.syncMarkerBytes = make([]byte, SyncSize)
	copy(r.syncMarkerBytes, marker)

	return nil
}

func (w *Writer) WriteHeader() (int, error) {
	totalwritten := 0
	var written int
	var err error

	magic := make([]byte, 0, 4)
	magic = append(magic, []byte("SEQ")...)
	magic = append(magic, byte(w.Header.Version))
	written, err = w.writer.Write(magic)
	totalwritten += written
	if err != nil {
		return totalwritten, err
	}

	written, err = w.writeString(w.Header.KeyClassName)
	totalwritten += written
	if err != nil {
		return totalwritten, err
	}

	written, err = w.writeString(w.Header.ValueClassName)
	totalwritten += written
	if err != nil {
		return totalwritten, err
	}

	written, err = w.writeBoolean(w.Header.Compression == RecordCompression)
	totalwritten += written
	if err != nil {
		return totalwritten, err
	}

	written, err = w.writeBoolean(w.Header.Compression == BlockCompression)
	totalwritten += written
	if err != nil {
		return totalwritten, err
	}

	// TODO: DRY this out along with other COMPRESSION
	if w.Header.Compression != NoCompression {
		switch w.Header.CompressionCodec {
		case GzipCompression:
			w.Header.CompressionCodecClassName = GzipClassName
		case SnappyCompression:
			w.Header.CompressionCodecClassName = SnappyClassName
		}

		written, err = w.writeString(w.Header.CompressionCodecClassName)
		totalwritten += written
		if err != nil {
			return totalwritten, err
		}
	}

	written, err = w.writeMetadata()
	totalwritten += written
	if err != nil {
		return totalwritten, err
	}

	written, err = w.writeSyncMarker()
	totalwritten += written
	if err != nil {
		return totalwritten, err
	}

	return totalwritten, nil
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

	return w.writer.Write(w.Header.SyncMarker)
}

func (r *Reader) readMetadata() error {
	r.clear()
	b, err := r.consume(4)
	if err != nil {
		return err
	}

	pairs := int(binary.BigEndian.Uint32(b))
	if pairs < 0 || pairs > 1024 {
		return fmt.Errorf("sequencefile: invalid metadata pair count: %d", pairs)
	}

	metadata := make(map[string]string, pairs)
	for i := 0; i < pairs; i++ {
		key, err := r.readString()
		if err != nil {
			return err
		}

		value, err := r.readString()
		if err != nil {
			return err
		}

		metadata[key] = value
	}

	r.Header.Metadata = metadata
	return nil
}

func (w *Writer) writeMetadata() (int, error) {
	totalwritten := 0

	length := len(w.Header.Metadata)
	lengthbytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthbytes, uint32(length))
	written, err := w.writer.Write(lengthbytes)
	totalwritten += written
	if err != nil {
		return totalwritten, err
	}

	keys := make([]string, 0, len(w.Header.Metadata))
	for k, _ := range w.Header.Metadata {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		written, err = w.writeString(key)
		totalwritten += written
		if err != nil {
			return totalwritten, err
		}

		written, err = w.writeString(w.Header.Metadata[key])
		totalwritten += written
		if err != nil {
			return totalwritten, err
		}
	}
	return totalwritten, nil
}

func (r *Reader) readBoolean() (bool, error) {
	r.clear()
	flag, err := r.consume(1)
	if err != nil {
		return false, err
	}
	flagint := uint8(flag[0])

	if flagint == 0 {
		return false, nil
	} else {
		return true, nil
	}
}

func (w *Writer) writeBoolean(flag bool) (int, error) {
	if flag {
		return w.writer.Write([]byte{0x01})
	} else {
		return w.writer.Write([]byte{0x00})
	}
}

func (r *Reader) readString() (string, error) {
	r.clear()
	b, err := r.consume(1)
	if err != nil {
		return "", err
	}

	length := int(b[0])
	r.clear()
	b, err = r.consume(length)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func (w *Writer) writeString(s string) (int, error) {
	length := int64(len(s))
	buf := new(bytes.Buffer)
	_, err := WriteVInt(buf, length)
	if err != nil {
		return 0, err
	}

	_, err = buf.Write([]byte(s))
	if err != nil {
		return 0, err
	}

	return w.Write(buf.Bytes())
}
