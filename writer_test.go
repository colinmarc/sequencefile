package sequencefile

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteFile(t *testing.T) {

}

type stringSpec struct {
	s     string
	bytes []byte
}

var stringspecs = []stringSpec{
	{"", []byte{0x00}},
	{"foo", []byte{0x03, 0x66, 0x6f, 0x6f}},
	{"the quick brown fox jumped over the lazy dog", []byte{0x2c, 0x74, 0x68, 0x65, 0x20, 0x71, 0x75, 0x69,
		0x63, 0x6b, 0x20, 0x62, 0x72, 0x6f, 0x77, 0x6e, 0x20, 0x66, 0x6f, 0x78, 0x20, 0x6a, 0x75, 0x6d, 0x70,
		0x65, 0x64, 0x20, 0x6f, 0x76, 0x65, 0x72, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6c, 0x61, 0x7a, 0x79, 0x20,
		0x64, 0x6f, 0x67,
	}},
}

func TestWriteString(t *testing.T) {
	for _, spec := range stringspecs {
		t.Run(fmt.Sprintf("writing '%s'", spec.s), func(t *testing.T) {
			buf := new(bytes.Buffer)
			w := NewWriter(buf)
			_, err := w.writeString(spec.s)
			assert.NoError(t, err, "WriteString should return successfully")
			assert.Equal(t, spec.bytes, buf.Bytes())
		})
	}
}

type testCompressionSpec struct {
	SpecName                  string
	Compression               Compression
	CompressionCodec          CompressionCodec
	CompressionCodecClassName string
}

var testcompressionspecs = []testCompressionSpec{
	{
		SpecName:                  "NoCompression",
		Compression:               NoCompression,
		CompressionCodec:          SnappyCompression,
		CompressionCodecClassName: SnappyClassName,
	},
	{
		SpecName:                  "BlockCompression with SnappyCompression",
		Compression:               BlockCompression,
		CompressionCodec:          SnappyCompression,
		CompressionCodecClassName: SnappyClassName,
	},
	{
		SpecName:                  "RecordCompression with SnappyCompression",
		Compression:               RecordCompression,
		CompressionCodec:          SnappyCompression,
		CompressionCodecClassName: SnappyClassName,
	},
	{
		SpecName:                  "BlockCompression with GzipCompression",
		Compression:               BlockCompression,
		CompressionCodec:          GzipCompression,
		CompressionCodecClassName: GzipClassName,
	},
	{
		SpecName:                  "RecordCompression with GzipCompression",
		Compression:               RecordCompression,
		CompressionCodec:          GzipCompression,
		CompressionCodecClassName: GzipClassName,
	},
}

func TestWriteHeaderCompression(t *testing.T) {
	for _, spec := range testcompressionspecs {
		t.Run(spec.SpecName, func(t *testing.T) {
			buf := new(bytes.Buffer)
			writer := NewWriter(buf)

			writer.Header.Compression = spec.Compression
			writer.Header.CompressionCodec = spec.CompressionCodec

			err := writer.WriteHeader()
			assert.NoError(t, err, "it should write successfully")

			r := NewReader(buf)
			err = r.ReadHeader()
			assert.NoError(t, err, "it should read successfully")

			assert.Equal(t, spec.Compression, r.Header.Compression, "it should have the correct compression type")
			if spec.Compression != NoCompression {
				assert.Equal(t, spec.CompressionCodecClassName, r.Header.CompressionCodecClassName, "it should have the correct compression codec class name")
				assert.Equal(t, spec.CompressionCodec, r.Header.CompressionCodec, "it should have the correct compression codec")
			}
		})
	}
}

func TestWriteFullSequenceFiles(t *testing.T) {
	for _, spec := range testcompressionspecs {
		t.Run(spec.SpecName, func(t *testing.T) {
			buf := new(bytes.Buffer)
			writer := NewWriter(buf)
			writer.Header.Compression = spec.Compression
			writer.Header.CompressionCodec = spec.CompressionCodec
			writer.Header.SyncMarker = []byte{0xe3, 0xb0, 0x6, 0x12, 0x67, 0xd4, 0x3b, 0xe0, 0xf3, 0xa, 0x78, 0x78, 0xcf, 0x69, 0x29, 0x6e}

			err := writer.WriteHeader()
			assert.NoError(t, err, "Header should be written successfully")

			err = writer.Append(PutBytesWritable([]byte("Alice")), PutBytesWritable([]byte("Practice")))
			assert.NoError(t, err, "key/value should successfully append")

			err = writer.Append(PutBytesWritable([]byte("Bob")), PutBytesWritable([]byte("Hope")))
			assert.NoError(t, err, "key/value should successfully append")

			randsize := 1024*256 + 68 // the +68 to make sure we're not landing on a chunk boundary
			randbytes := make([]byte, randsize)
			_, err = rand.Read(randbytes)
			assert.NoError(t, err, "we should successfully fill the slice with random junk")

			err = writer.Append(PutBytesWritable([]byte("randombytes")), PutBytesWritable(randbytes))
			assert.NoError(t, err, "key/value should successfully append")

			longstring := []byte(strings.Repeat("a", 1024*256+42))
			err = writer.Append(PutBytesWritable([]byte("longstring")), PutBytesWritable(longstring))
			assert.NoError(t, err, "key/value should successfully append")

			for i := 0; i < 1000; i++ {
				err = writer.Append(PutBytesWritable([]byte(fmt.Sprintf("key %d", i))), PutBytesWritable([]byte(fmt.Sprintf("value %d", i))))
				assert.NoError(t, err, "key/value should successfully append")
			}

			err = writer.Flush()
			assert.NoError(t, err, "flush should succeed")

			reader := NewReader(buf)
			err = reader.ReadHeader()

			assert.NoError(t, err, "should successfully read the header")
			assert.Equal(t, writer.Header.SyncMarker, reader.Header.SyncMarker, "sync marker read should be the same as sync marker written")

			success := reader.Scan()
			assert.True(t, success, "we successfully read a key/value pair")

			assert.Equal(t, []byte("Alice"), BytesWritable(reader.Key()), "we read the correct key")
			assert.Equal(t, []byte("Practice"), BytesWritable(reader.Value()), "we read the correct value")

			success = reader.Scan()
			assert.True(t, success, "we successfully read a key/value pair")
			assert.Equal(t, []byte("Bob"), BytesWritable(reader.Key()), "we read the correct key")
			assert.Equal(t, []byte("Hope"), BytesWritable(reader.Value()), "we read the correct value")

			success = reader.Scan()
			assert.True(t, success, "we successfully read a key/value pair")
			assert.Equal(t, []byte("randombytes"), BytesWritable(reader.Key()), "we read the correct key")
			assert.Equal(t, randbytes, BytesWritable(reader.Value()), "we read the correct value")

			success = reader.Scan()
			assert.True(t, success, "we successfully read a key/value pair")
			assert.Equal(t, []byte("longstring"), BytesWritable(reader.Key()), "we read the correct key")
			assert.Equal(t, longstring, BytesWritable(reader.Value()), "we read the correct value")

			for i := 0; i < 1000; i++ {
				success = reader.Scan()
				assert.True(t, success, "we successfully read a key/value pair")
				assert.Equal(t, []byte(fmt.Sprintf("key %d", i)), BytesWritable(reader.Key()), "we read the correct key")
				assert.Equal(t, []byte(fmt.Sprintf("value %d", i)), BytesWritable(reader.Value()), "we read the correct value")
			}

			assert.Equal(t, []byte{}, buf.Bytes(), "there should be nothing left in the buffer")

		})
	}
}
