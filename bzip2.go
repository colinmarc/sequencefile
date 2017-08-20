package sequencefile

import (
	"bytes"
	"io"

	"github.com/dsnet/compress/bzip2"
)

type bzip2Reader struct {
	io.Reader
}

func newBzip2Reader(in io.Reader) *bzip2Reader {
	reader := &bzip2Reader{}
	reader.Reset(in)
	return reader
}

// Reset implements decompressor using a bzip2.Reader
func (r *bzip2Reader) Reset(in io.Reader) error {
	br, err := bzip2.NewReader(in, nil)
	if err != nil {
		return err
	}

	r.Reader = br
	return nil
}

// Close implements decompressor. The underlying reader must be closed independently.
func (r *bzip2Reader) Close() error {
	return nil
}

type bzip2Compressor struct {
	bz  *bzip2.Writer
	buf bytes.Buffer
}

func (c *bzip2Compressor) compress(src []byte) ([]byte, error) {
	if c.bz != nil {
		c.buf.Reset()
		c.bz.Reset(&c.buf)
	} else {
		bz, err := bzip2.NewWriter(&c.buf,
			&bzip2.WriterConfig{Level: bzip2.DefaultCompression})
		if err != nil {
			return nil, err
		}

		c.bz = bz
	}

	if _, err := c.bz.Write(src); err != nil {
		return nil, err
	}

	if err := c.bz.Close(); err != nil {
		return nil, err
	}

	return c.buf.Bytes(), nil
}
