package sequencefile

import (
	"compress/zlib"
	"io"
)

type zlibReaderWrapper struct {
	io.ReadCloser
}

func newZlibReaderWrapper(src io.Reader) (*zlibReaderWrapper, error) {
	r, err := zlib.NewReader(src)
	if err != nil {
		return nil, err
	}

	return &zlibReaderWrapper{r}, nil
}

// Reset implements decompressor.Reset, papering over the difference in
// interface.
func (z *zlibReaderWrapper) Reset(r io.Reader) error {
	// The zlib docs guarantee that the ReadCloser returned by NewReader will also
	// implement zlib.Resetter, so this type assertion should be safe.
	return z.ReadCloser.(zlib.Resetter).Reset(r, nil)
}
