package sequencefile

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/golang/snappy"

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

// TODO: fullSequenceFileSpec + randomly generated keys/values
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

func TestWriteRecordCompressedGzip(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := NewWriter(buf)
	writer.Header.Compression = RecordCompression
	writer.Header.CompressionCodec = GzipCompression

	_, err := writer.WriteHeader()
	assert.NoError(t, err, "Header should be written successfully")

	written, err := writer.Append(PutBytesWritable([]byte("foo")), PutBytesWritable([]byte("bar")))
	assert.NoError(t, err, "key/value should successfully append")
	t.Logf("wrote (foo, bar): %d bytes", written)

	written, err = writer.Append(PutBytesWritable([]byte("foo1")), PutBytesWritable([]byte("bar1")))
	assert.NoError(t, err, "key/value should successfully append")
	t.Logf("wrote (foo1, bar1): %d bytes", written)

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

func TestWriteRecordCompressedSnappy(t *testing.T) {
	buf := new(bytes.Buffer)
	writer := NewWriter(buf)
	writer.Header.Compression = RecordCompression
	writer.Header.CompressionCodec = SnappyCompression

	_, err := writer.WriteHeader()
	assert.NoError(t, err, "Header should be written successfully")

	_, err = writer.Append(PutBytesWritable([]byte("foo")), PutBytesWritable([]byte("bar")))
	assert.NoError(t, err, "key/value should successfully append")

	_, err = writer.Append(PutBytesWritable([]byte("foo1")), PutBytesWritable([]byte("bar1")))
	assert.NoError(t, err, "key/value should successfully append")

	randsize := 1024*256 + 68 // the +68 to make sure we're not landing on a chunk boundary
	randbytes := make([]byte, randsize)
	_, err = rand.Read(randbytes)
	assert.NoError(t, err, "we should successfully fill the slice with random junk")

	_, err = writer.Append(PutBytesWritable([]byte("randombytes")), PutBytesWritable(randbytes))
	assert.NoError(t, err, "key/value should successfully append")

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

	success = reader.Scan()
	assert.True(t, success, "we successfully read a key/value pair")
	assert.Equal(t, []byte("randombytes"), BytesWritable(reader.Key()), "we read the correct key")
	assert.Equal(t, randbytes, BytesWritable(reader.Value()), "we read the correct value")

	assert.Equal(t, []byte{}, buf.Bytes(), "there should be nothing left in the buffer")

}

func TestDoIUnderstandSnappy(t *testing.T) {
	var err error
	key := PutBytesWritable([]byte("foo1"))
	value := PutBytesWritable([]byte("bar1"))
	buf := new(bytes.Buffer)

	value_snappy_frame := snappy.Encode(nil, value)
	value_snappy_frame_size := uint32(len(value_snappy_frame))
	value_snappy_frame_size_bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(value_snappy_frame_size_bytes, value_snappy_frame_size)

	value_decompressed_size := uint32(8791328)
	value_decompressed_size_bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(value_decompressed_size_bytes, value_decompressed_size)

	framed_value_snappy_bytes := make([]byte, 0, value_snappy_frame_size+8)
	framed_value_snappy_bytes = append(framed_value_snappy_bytes, value_decompressed_size_bytes...)
	framed_value_snappy_bytes = append(framed_value_snappy_bytes, value_snappy_frame_size_bytes...)
	framed_value_snappy_bytes = append(framed_value_snappy_bytes, value_snappy_frame...) // this is the full "value" bytes for the record.
	framed_value_snappy_bytes_len := uint32(len(framed_value_snappy_bytes))

	key_size := uint32(len(key))
	key_size_bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(key_size_bytes, key_size)

	record_size := key_size + framed_value_snappy_bytes_len
	record_size_bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(record_size_bytes, record_size)

	// write out record_size_bytes, key_size_bytes, key, framed_value_snappy_bytes
	_, err = buf.Write(record_size_bytes)
	assert.NoError(t, err, "writing record_size_bytes should not error")
	_, err = buf.Write(key_size_bytes)
	assert.NoError(t, err, "writing key_size_bytes should not error")
	_, err = buf.Write(key)
	assert.NoError(t, err, "writing key should not error")
	_, err = buf.Write(framed_value_snappy_bytes)
	assert.NoError(t, err, "writing the actual snappy frame wrapped fully whatever bytes should not error")

	//assert.Equal(t, []byte{}, buf.Bytes(), "just checking to see what is even in the bytes")

	reader := NewReaderCompression(buf, RecordCompression, SnappyCompression)
	ret := reader.Scan()
	assert.True(t, ret, "scan should find a record")
	assert.Equal(t, []byte("foo1"), BytesWritable(reader.Key()), "key should be foo1")
	assert.Equal(t, []byte("bar1"), BytesWritable(reader.Value()), "value should be bar1")

	assert.Equal(t, []byte{}, buf.Bytes(), "the buffer should be empty now")

}
