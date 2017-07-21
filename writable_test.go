package sequencefile

import (
	"strconv"
	"testing"

	"bytes"

	"github.com/stretchr/testify/assert"
)

// To generate the values used in these tests:
// scala> val baos = new java.io.ByteArrayOutputStream()
// scala> val dos = new java.io.DataOutputStream(baos)
// scala> baos.reset; new org.apache.hadoop.io.IntWritable(42).write(dos)
// scala> javax.xml.bind.DatatypeConverter.printHexBinary(baos.toByteArray)

var bytesWritables = []struct {
	b        []byte
	expected []byte
}{
	{[]byte{0x00, 0x00, 0x00, 0x00}, []byte("")},
	{[]byte{0x00, 0x00, 0x00, 0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72}, []byte("foobar")},
}

func TestBytesWritable(t *testing.T) {
	for _, spec := range bytesWritables {
		t.Run(string(spec.expected), func(t *testing.T) {
			assert.Equal(t, spec.expected, BytesWritable(spec.b), "BytesWritable should unwrap correctly")
		})
	}
}

var texts = []struct {
	b        []byte
	expected string
}{
	{[]byte{0x00}, ""},
	{[]byte{0x06, 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72}, "foobar"},
}

func TestText(t *testing.T) {
	for _, spec := range texts {
		t.Run(spec.expected, func(t *testing.T) {
			assert.Equal(t, spec.expected, Text(spec.b), "Text should unwrap correctly")
		})
	}
}

var intWritables = []struct {
	b        []byte
	expected int32
}{
	{[]byte{0x00, 0x00, 0x00, 0x00}, 0},
	{[]byte{0x00, 0x00, 0x00, 0x2A}, 42},
	{[]byte{0xFF, 0xFF, 0xFC, 0x18}, -1000},
	{[]byte{0x7F, 0xFF, 0xFF, 0xFF}, 2147483647},
	{[]byte{0x80, 0x00, 0x00, 0x01}, -2147483647},
}

func TestIntWritable(t *testing.T) {
	for _, spec := range intWritables {
		t.Run(strconv.FormatInt(int64(spec.expected), 10), func(t *testing.T) {
			assert.Equal(t, spec.expected, IntWritable(spec.b), "IntWritable should unwrap correctly")
		})
	}
}

var longWritables = []struct {
	b        []byte
	expected int64
}{
	{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 0},
	{[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A}, 42},
	{[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFC, 0x18}, -1000},
	{[]byte{0x00, 0x00, 0x00, 0x00, 0x7F, 0xFF, 0xFF, 0xFF}, 2147483647},
	{[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x80, 0x00, 0x00, 0x01}, -2147483647},
	{[]byte{0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, 576460752303423488},
	{[]byte{0xF8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, -576460752303423488},
}

func TestLongWritable(t *testing.T) {
	for _, spec := range longWritables {
		t.Run(strconv.FormatInt(spec.expected, 10), func(t *testing.T) {
			assert.Equal(t, spec.expected, LongWritable(spec.b), "LongWritable should unwrap correctly")
		})
	}
}

func assertWriteWritable(t *testing.T, className string, v interface{}, expected []byte) {
	w, err := NewWritableWriter(className)
	assert.NoError(t, err)

	var buf bytes.Buffer
	err = w(&buf, v)
	assert.NoError(t, err)
	assert.Equal(t, expected, buf.Bytes())
}

func TestWriteWritable(t *testing.T) {
	for _, spec := range bytesWritables {
		assertWriteWritable(t, BytesWritableClassName, spec.expected, spec.b)
	}
	for _, spec := range texts {
		assertWriteWritable(t, TextClassName, spec.expected, spec.b)
	}
	for _, spec := range intWritables {
		assertWriteWritable(t, IntWritableClassName, spec.expected, spec.b)
	}
	for _, spec := range longWritables {
		assertWriteWritable(t, LongWritableClassName, spec.expected, spec.b)
	}

	// Errors.
	var buf bytes.Buffer
	w, err := NewWritableWriter("fake")
	assert.Nil(t, w)
	assert.Error(t, err)
	w, err = NewWritableWriter(BytesWritableClassName)
	assert.NotNil(t, w)
	assert.NoError(t, err)
	assert.NoError(t, w(&buf, []byte{}))
	assert.Error(t, w(&buf, ""))
	assert.Error(t, w(&buf, int32(1)))
	assert.Error(t, w(&buf, int64(1)))
	assert.Error(t, w(&buf, []string{}))
	w, err = NewWritableWriter(TextClassName)
	assert.NotNil(t, w)
	assert.NoError(t, err)
	assert.Error(t, w(&buf, []byte{}))
	assert.NoError(t, w(&buf, ""))
	assert.Error(t, w(&buf, int32(1)))
	assert.Error(t, w(&buf, int64(1)))
	assert.Error(t, w(&buf, []string{}))
	w, err = NewWritableWriter(IntWritableClassName)
	assert.NotNil(t, w)
	assert.NoError(t, err)
	assert.Error(t, w(&buf, []byte{}))
	assert.Error(t, w(&buf, ""))
	assert.NoError(t, w(&buf, int32(1)))
	assert.Error(t, w(&buf, int64(1)))
	assert.Error(t, w(&buf, []string{}))
	w, err = NewWritableWriter(LongWritableClassName)
	assert.NotNil(t, w)
	assert.NoError(t, err)
	assert.Error(t, w(&buf, []byte{}))
	assert.Error(t, w(&buf, ""))
	assert.Error(t, w(&buf, int32(1)))
	assert.NoError(t, w(&buf, int64(1)))
	assert.Error(t, w(&buf, []string{}))
}
