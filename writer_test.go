package sequencefile

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteFile(t *testing.T) {

}

type stringSpec struct {
	s     string
	bytes []byte
}

var strings = []stringSpec{
	{"", []byte{0x00}},
	{"foo", []byte{0x03, 0x66, 0x6f, 0x6f}},
	{"the quick brown fox jumped over the lazy dog", []byte{0x2c, 0x74, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69,
		0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f, 0x77, 0x6e, 0x20, 0x66, 0x6f, 0x78, 0x20, 0x6a, 0x75, 0x6d, 0x70,
		0x65, 0x64, 0x20, 0x6f, 0x76, 0x65, 0x72, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6c, 0x61, 0x7a, 0x79, 0x20,
		0x64, 0x6f, 0x67,
	}},
}

func TestWriteString(t *testing.T) {
	for _, spec := range strings {
		t.Run(fmt.Sprintf("writing '%s'", spec.s), func(t *testing.T) {
			buf := new(bytes.Buffer)
			w := NewWriter(buf)
			_, err := w.writeString(spec.s)
			assert.NoError(t, err, "WriteString should return successfully")
			assert.Equal(t, spec.bytes, buf.Bytes())
		})
	}
}

func TestWriteFullSequenceFile(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := NewWriter(buf)

	_, err := writer.WriteHeader()
	assert.NoError(t, err, "Header should be written successfully")

	written, err := writer.Append(PutBytesWritable([]byte("foo")), PutBytesWritable([]byte("bar")))
	assert.NoError(t, err, "key/value should successfully append")
	assert.Equal(t, 22, written, "it should write the correct number of bytes")

	written, err = writer.Append(PutBytesWritable([]byte("foo1")), PutBytesWritable([]byte("bar1")))
	assert.NoError(t, err, "key/value should successfully append")
	assert.Equal(t, 24, written, "it should write the correct number of bytes")

	reader := NewReader(buf)
	err = reader.ReadHeader()
	assert.NoError(t, err, "should successfully read the header")
	success := reader.Scan()
	assert.True(t, success, "we successfully read a key/value pair")
	assert.Equal(t, []byte("foo"), BytesWritable(reader.Key()), "we read the correct key")
	assert.Equal(t, []byte("bar"), BytesWritable(reader.Value()), "we read the correct value")

	success = reader.Scan()
	assert.True(t, success, "we successfully read a key/value pair")
	assert.Equal(t, []byte("foo1"), BytesWritable(reader.Key()), "we read the correct key")
	assert.Equal(t, []byte("bar1"), BytesWritable(reader.Value()), "we read the correct value")

	assert.Equal(t, []byte{}, buf.Bytes(), "there should be nothing left in the buffer")
}
