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
}

func (z zstdCompressor) compress(src []byte) ([]byte, error) {
	var out bytes.Buffer
	enc, err := zstd.NewWriter(&out)
	if err != nil {
		return nil, err
	}
	in := bytes.NewReader(src)
	_, err = io.Copy(enc, in)
	if err != nil {
		enc.Close()
		return nil, err
	}
	enc.Close()
	return out.Bytes(), nil
}
