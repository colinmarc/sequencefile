// Package sequencefile provides functionality for reading and writing Hadoop's
// SequenceFile format, documented here: http://goo.gl/sOSJmJ
package sequencefile

import "io"

type Compression int
type CompressionCodec int

const (
	SyncSize = 16

	GzipClassName   = "org.apache.hadoop.io.compress.GzipCodec"
	SnappyClassName = "org.apache.hadoop.io.compress.SnappyCodec"
	ZlibClassName   = "org.apache.hadoop.io.compress.DefaultCodec"
	ZstdClassName   = "org.apache.hadoop.io.compress.ZStandardCodec"
	Bzip2ClassName  = "org.apache.hadoop.io.compress.BZip2Codec"
)

const (
	NoCompression Compression = iota + 1
	RecordCompression
	BlockCompression
)

const (
	GzipCompression CompressionCodec = iota + 1
	SnappyCompression
	ZlibCompression
	ZstdCompression
	Bzip2Compression
)

type decompressor interface {
	Read(p []byte) (n int, err error)
	Reset(r io.Reader) error
	Close() error
}
