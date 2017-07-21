package sequencefile

import (
	"encoding/binary"
	"io"
)

// ReadVInt reads an int64 encoded in hadoop's "VInt" format, described and
// implemented here: https://goo.gl/1h4mrG. It does at most two reads to the
// underlying io.Reader.
func ReadVInt(r io.Reader) (int64, error) {
	lenByte, err := mustReadByte(r)
	if err != nil {
		return 0, err
	}

	l := int8(lenByte)
	var remaining int
	var negative bool
	if l >= -112 {
		return int64(l), nil
	} else if l >= -120 {
		remaining = int(-112 - l)
		negative = false
	} else {
		remaining = int(-120 - l)
		negative = true
	}

	var res uint64
	buf := make([]byte, remaining)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return 0, err
	}

	for _, b := range buf {
		res = (res << 8) | uint64(b)
	}

	if negative {
		res = ^res
	}

	return int64(res), nil
}

func mustReadByte(r io.Reader) (byte, error) {
	var b byte
	var err error

	if br, ok := r.(io.ByteReader); ok {
		b, err = br.ReadByte()
	} else {
		buf := make([]byte, 1)
		_, err = io.ReadFull(r, buf)
		b = buf[0]
	}

	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}

	return b, err
}

// WriteVInt writes an int64 encoded in Hadoop's "VInt" format.
func WriteVInt(w io.Writer, i int64) (err error) {
	if i >= -112 && i < 127 {
		_, err = w.Write([]byte{byte(i)})
		return err
	}

	bits := uint64(i)
	if i < 0 {
		bits = ^bits
	}

	var bs [8]byte
	binary.BigEndian.PutUint64(bs[:], bits)

	for n, b := range bs {
		if b != 0 {
			if i < 0 {
				_, err = w.Write([]byte{byte(-128 + n)})
			} else {
				_, err = w.Write([]byte{byte(-120 + n)})
			}
			if err != nil {
				return err
			}
			_, err = w.Write(bs[n:])
			return err
		}
	}
	panic("Unreachable")
}
