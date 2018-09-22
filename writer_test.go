package sequencefile

import (
	"bytes"
	"errors"
	"hash/fnv"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type writePair struct {
	k, v interface{}
}

func assertWrite(t *testing.T, cfg *WriterConfig, pairs []writePair) []byte {
	var buf bytes.Buffer
	cfg.Writer = &buf
	w, err := NewWriter(cfg)
	require.NoError(t, err)

	for _, p := range pairs {
		err = w.Append(p.k, p.v)
		assert.NoError(t, err)
	}

	w.Close()
	return buf.Bytes()
}

func TestWriter(t *testing.T) {
	buf := assertWrite(t,
		&WriterConfig{
			KeyClass:   BytesWritableClassName,
			ValueClass: BytesWritableClassName,
			Rand:       rand.New(rand.NewSource(42)),
		},
		[]writePair{
			{[]byte("Alice"), []byte("Practice")},
			{[]byte("Bob"), []byte("Hope")},
		},
	)

	fbytes, err := ioutil.ReadFile("testdata/uncompressed_written.sequencefile")
	require.NoError(t, err)
	assert.Equal(t, fbytes, buf)
}

func TestWriterCompressionSettings(t *testing.T) {
	compressions := []struct {
		compression Compression
		codec       CompressionCodec
		ok          bool
		level		int
	}{
		{NoCompression, 0, true, 0},
		{NoCompression, GzipCompression, true, 0},
		{RecordCompression, GzipCompression, true, 0},
		{RecordCompression, GzipCompression, true, 1},
		{RecordCompression, GzipCompression, false, 11},
		{RecordCompression, 0, false, 0},
		{RecordCompression, SnappyCompression, true, 0},
		{BlockCompression, GzipCompression, true, 0},
		{BlockCompression, GzipCompression, true, 1},
		{BlockCompression, GzipCompression, false, 11},
		{BlockCompression, 0, false, 0},
		{BlockCompression, SnappyCompression, true, 0},
	}

	for _, cmp := range compressions {
		var buf bytes.Buffer
		w, err := NewWriter(&WriterConfig{
			Writer:           &buf,
			Compression:      cmp.compression,
			CompressionCodec: cmp.codec,
			CompressionLevel: cmp.level,
		})
		if cmp.ok {
			assert.NoError(t, err)
			w.Close()
		} else {
			assert.Error(t, err)
		}
	}
}

type compressionSpec struct {
	compression Compression
	codec       CompressionCodec
	level 		int
}

func TestWriterRoundTrip(t *testing.T) {
	compressions := []compressionSpec{
		{NoCompression, 0, 0},
		{RecordCompression, GzipCompression, 0},
		{RecordCompression, GzipCompression, 1},
		{RecordCompression, SnappyCompression, 0},
		{BlockCompression, GzipCompression, 0},
		{BlockCompression, GzipCompression, 1},
		{BlockCompression, SnappyCompression, 0},
	}

	pairs := []writePair{
		{"foo", int32(42)},
		{"bar", int32(-1)},
		{"iggy", int32(12345678)},
	}

	seed := time.Now().UnixNano()
	digests := map[uint64]bool{}

	for _, cmp := range compressions {
		buf := assertWrite(t,
			&WriterConfig{
				Compression:      cmp.compression,
				CompressionCodec: cmp.codec,
				CompressionLevel: cmp.level,
				KeyClass:         TextClassName,
				ValueClass:       IntWritableClassName,
				Rand:             rand.New(rand.NewSource(seed)),
			},
			pairs,
		)

		h := fnv.New64()
		h.Write(buf)
		digest := h.Sum64()
		_, found := digests[digest]
		assert.False(t, found, "Different compressions should have different results")
		digests[digest] = true

		r := NewReader(bytes.NewBuffer(buf))
		require.NoError(t, r.ReadHeader())
		for _, p := range pairs {
			assert.True(t, r.Scan())
			assert.Equal(t, p.k, Text(r.Key()))
			assert.Equal(t, p.v, IntWritable(r.Value()))
		}
		assert.False(t, r.Scan())
		assert.NoError(t, r.Err())
	}
}

func TestWriterLong(t *testing.T) {
	compressions := []compressionSpec{
		{NoCompression, 0, 0},
		{RecordCompression, GzipCompression, 0},
		{BlockCompression, SnappyCompression, 0},
		{BlockCompression, GzipCompression, 1},
	}

	for _, cmp := range compressions {
		var pairs []writePair
		value := bytes.Repeat([]byte{0}, 2000)
		for i := 0; i < 2000; i++ {
			pairs = append(pairs, writePair{int64(i), value})
		}

		buf := assertWrite(t,
			&WriterConfig{
				KeyClass:         LongWritableClassName,
				ValueClass:       BytesWritableClassName,
				Compression:      cmp.compression,
				CompressionCodec: cmp.codec,
				CompressionLevel: cmp.level,
			},
			pairs,
		)

		r := NewReader(bytes.NewBuffer(buf))
		require.NoError(t, r.ReadHeader())
		for _, p := range pairs {
			assert.True(t, r.Scan())
			assert.Equal(t, p.k, LongWritable(r.Key()))
			assert.Equal(t, p.v, BytesWritable(r.Value()))
		}
		assert.False(t, r.Scan())
		assert.NoError(t, r.Err())
	}
}

type errorWriter struct {
	buf    bytes.Buffer
	didErr bool
}

func (e *errorWriter) Write(buf []byte) (int, error) {
	if !e.didErr && e.buf.Len() >= 114 {
		// Simulate an error.
		e.didErr = true
		return 0, errors.New("test error")
	}
	return e.buf.Write(buf)
}

func TestWriterError(t *testing.T) {
	ew := &errorWriter{}
	w, err := NewWriter(&WriterConfig{Writer: ew})
	require.NoError(t, err)

	// No error yet.
	err = w.Append([]byte{0}, []byte{0})
	assert.NoError(t, err)

	// Now we've reached an error.
	err = w.Append([]byte{1}, []byte{1})
	assert.Error(t, err)
	size := ew.buf.Len()

	// Subsequent appends should error, not panic, and not cause more writes.
	err = w.Append([]byte{2}, []byte{2})
	assert.Error(t, err)
	assert.Equal(t, size, ew.buf.Len())

	// Closing should error.
	assert.Error(t, w.Close())
}
