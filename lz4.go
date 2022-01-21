package sequencefile

import (
	"encoding/binary"
	"fmt"
	"github.com/pierrec/lz4"
	"io"
)

type lz4ReaderWrapper struct {
	src io.Reader
	out []byte
}

func newLz4ReaderWrapper(src io.Reader) (*lz4ReaderWrapper, error) {
	z := &lz4ReaderWrapper{}

	if err := z.Reset(src); err != nil {
		return nil, err
	}

	return z, nil
}

func (z *lz4ReaderWrapper) refill(off int) (int, error) {
	var hdr [4]byte
	_, err := io.ReadFull(z.src, hdr[:])
	if err != nil {
		return 0, fmt.Errorf("io.ReadFull(hdr[4]): %v", err)
	}
	inLen := binary.BigEndian.Uint32(hdr[:4])
	if inLen == 0 {
		return 0, nil
	}
	inBuf := make([]byte, inLen)
	_, err = io.ReadFull(z.src, inBuf)
	if err != nil {
		return 0, fmt.Errorf("io.ReadFull: %v", err)
	}
    n, err := lz4.UncompressBlock(inBuf, z.out[off:])
	if err != nil {
		return 0, fmt.Errorf("lz4.UncompressBlock: %v", err)
	}
	return n, nil
}

func (z *lz4ReaderWrapper) Read(p []byte) (n int, err error) {
	if z.out == nil {
		return 0, io.EOF
	}

	if len(z.out) > len(p) {
		n = len(p)
		copy(p, z.out[:n])
		z.out = z.out[n:]
		return n, nil
	} else {
		n = len(z.out)
		copy(p[:n], z.out)
		z.out = nil
		return n, io.EOF
	}
}

func (z *lz4ReaderWrapper) Reset(r io.Reader) error {
	z.src = r
	z.out = nil

	// read the full length and allocate a buffer
	var hdr [4]byte
	_, err := io.ReadFull(r, hdr[:])
	if err == io.ErrUnexpectedEOF {
		return io.EOF
	}
	outLen := binary.BigEndian.Uint32(hdr[:4])
	z.out = make([]byte, outLen)

	for off := 0; off < int(outLen); {
		n, err := z.refill(off)
		if err != nil {
			return err
		}
		off += n
	}

	// sometimes there are 4 extra 0 bytes appended to the end...?
	var extra [16]byte
	for {
		_, err = r.Read(extra[:])
		if err == io.EOF {
			break
		}
	}

	return nil
}

func (z *lz4ReaderWrapper) Close() error {
	return nil
}
