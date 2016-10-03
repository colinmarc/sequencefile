package sequencefile

import (
	"bytes"
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

	r.Header.SyncMarker = make([]byte, SyncSize)
	copy(r.Header.SyncMarker, marker)
	r.syncMarkerBytes = make([]byte, SyncSize)
	copy(r.syncMarkerBytes, marker)

	return nil
}

func (w *Writer) WriteHeader() error {
	var err error

	magic := make([]byte, 0, 4)
	magic = append(magic, []byte("SEQ")...)
	magic = append(magic, byte(w.Header.Version))
	_, err = w.writer.Write(magic)
	if err != nil {
		return err
	}

	_, err = w.writeString(w.Header.KeyClassName)
	if err != nil {
		return err
	}

	_, err = w.writeString(w.Header.ValueClassName)
	if err != nil {
		return err
	}

	_, err = w.writeBoolean(w.Header.Compression == RecordCompression)
	if err != nil {
		return err
	}

	_, err = w.writeBoolean(w.Header.Compression == BlockCompression)
	if err != nil {
		return err
	}

	if w.Header.Compression != NoCompression {
		switch w.Header.CompressionCodec {
		case GzipCompression:
			w.Header.CompressionCodecClassName = GzipClassName
		case SnappyCompression:
			w.Header.CompressionCodecClassName = SnappyClassName
		}

		_, err = w.writeString(w.Header.CompressionCodecClassName)
		if err != nil {
			return err
		}
	}

	_, err = w.writeMetadata()
	if err != nil {
		return err
	}

	_, err = w.writeSyncMarker()
	if err != nil {
		return err
	}

	return nil
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
	totalWritten := 0

	length := len(w.Header.Metadata)
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(length))
	written, err := w.writer.Write(lengthBytes)
	totalWritten += written
	if err != nil {
		return totalWritten, err
	}

	keys := make([]string, 0, len(w.Header.Metadata))
	for k, _ := range w.Header.Metadata {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		written, err = w.writeString(key)
		totalWritten += written
		if err != nil {
			return totalWritten, err
		}

		written, err = w.writeString(w.Header.Metadata[key])
		totalWritten += written
		if err != nil {
			return totalWritten, err
		}
	}
	return totalWritten, nil
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
	length, err := ReadVInt(r.reader)
	if err != nil {
		return "", err
	}

	r.clear()
	b, err := r.consume(int(length))
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

	return w.writer.Write(buf.Bytes())
}
