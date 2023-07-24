package sequencefile

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	BytesWritableClassName = "org.apache.hadoop.io.BytesWritable"
	TextClassName          = "org.apache.hadoop.io.Text"
	IntWritableClassName   = "org.apache.hadoop.io.IntWritable"
	LongWritableClassName  = "org.apache.hadoop.io.LongWritable"
)

// BytesWritable unwraps a hadoop BytesWritable and returns the actual bytes.
func BytesWritable(b []byte) []byte {
	return b[4:]
}

// Text unwraps a Text and returns the deserialized string.
func Text(b []byte) string {
	buf := bytes.NewBuffer(b)
	n, err := ReadVInt(buf)
	if err != nil {
		panic(fmt.Sprintf("sequencefile: unwrapping Text: %s", err))
	}

	if int(n) != buf.Len() {
		panic("sequencefile: unwrapping Text: bad length")
	}

	return buf.String()
}

// IntWritable unwraps an IntWritable and returns the deserialized int32.
func IntWritable(b []byte) int32 {
	return int32(binary.BigEndian.Uint32(b))
}

// LongWritable unwraps an LongWritable and returns the deserialized int64.
func LongWritable(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

// A WritableWriter knows how to write data wrapped in Hadoop Writables.
//
// Each WritableWriter understands just a single type of data.
type WritableWriter func(io.Writer, interface{}) error

type writableWriteError struct {
	class    string
	needType string
	value    interface{}
}

func (e *writableWriteError) Error() string {
	return fmt.Sprintf("Class %s requires data of type %s", e.class, e.needType)
}

func writeBytes(w io.Writer, value interface{}) (err error) {
	v, ok := value.([]byte)
	if !ok {
		return &writableWriteError{BytesWritableClassName, "[]byte", value}
	}

	var bs [4]byte
	binary.BigEndian.PutUint32(bs[:], uint32(len(v)))
	if _, err = w.Write(bs[:]); err != nil {
		return
	}
	_, err = w.Write(v)
	return
}

func writeText(w io.Writer, value interface{}) (err error) {
	v, ok := value.(string)
	if !ok {
		return &writableWriteError{TextClassName, "string", value}
	}

	if err = WriteVInt(w, int64(len(v))); err != nil {
		return
	}
	_, err = w.Write([]byte(v))
	return
}

func writeInt(w io.Writer, value interface{}) (err error) {
	v, ok := value.(int32)
	if !ok {
		return &writableWriteError{IntWritableClassName, "int32", value}
	}

	var bs [4]byte
	binary.BigEndian.PutUint32(bs[:], uint32(v))
	_, err = w.Write(bs[:])
	return
}

func writeLong(w io.Writer, value interface{}) (err error) {
	v, ok := value.(int64)
	if !ok {
		return &writableWriteError{LongWritableClassName, "int64", value}
	}

	var bs [8]byte
	binary.BigEndian.PutUint64(bs[:], uint64(v))
	_, err = w.Write(bs[:])
	return
}

// NewWritableWriter gets a WritableWriter for a given Hadoop class name.
func NewWritableWriter(className string) (WritableWriter, error) {
	switch className {
	case BytesWritableClassName:
		return writeBytes, nil
	case TextClassName:
		return writeText, nil
	case IntWritableClassName:
		return writeInt, nil
	case LongWritableClassName:
		return writeLong, nil
	default:
		return nil, fmt.Errorf("unknown writable class %s", className)
	}
}
