package sequencefile

import (
	"compress/bzip2"
	"io"
)

type bzip2Reader struct {
	io.Reader
}

func newBZip2Reader(in io.Reader) *bzip2Reader {
	reader := &bzip2Reader{}
	reader.Reset(in)
	return reader
}

// Reset implements decompressor using a bzip2.Reader
func (r *bzip2Reader) Reset(in io.Reader) error {
	r.Reader = bzip2.NewReader(in)
	return nil
}

// Close implements decompressor. The underlying reader must be closed independently.
func (r *bzip2Reader) Close() error {
	return nil
}
