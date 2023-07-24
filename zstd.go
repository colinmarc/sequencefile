package sequencefile

import (
	"bytes"
	"io"

	"github.com/klauspost/compress/zstd"
)

type zstdReaderWrapper struct {
	*zstd.Decoder
}

func newZstdReaderWrapper(src io.Reader) (*zstdReaderWrapper, error) {
	r, err := zstd.NewReader(src)
	if err != nil {
		return nil, err
	}

	return &zstdReaderWrapper{r}, nil
}

func (z *zstdReaderWrapper) Close() error {
	z.Decoder.Close()
	return nil
}

type zstdCompressor struct {
	zc  *zstd.Encoder
	buf bytes.Buffer
}

func (c zstdCompressor) compress(src []byte) ([]byte, error) {
	if c.zc != nil {
		c.buf.Reset()
		c.zc.Reset(&c.buf)
	} else {
		zc, err := zstd.NewWriter(&c.buf)
		if err != nil {
			return nil, err
		}

		c.zc = zc
	}

	if _, err := c.zc.Write(src); err != nil {
		return nil, err
	}

	if err := c.zc.Close(); err != nil {
		return nil, err
	}

	return c.buf.Bytes(), nil
}
