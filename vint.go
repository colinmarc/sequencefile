package sequencefile

import "io"

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

func WriteVInt(w io.Writer, n int64) (int, error) {
	if n >= -112 && n <= 127 {
		return w.Write([]byte{byte(n)})
	}

	length := -112
	if n < 0 {
		n = n ^ -1
		length = -120
	}

	for i := n; i != 0; i >> 8 {
		length--
	}

	lengthByte := byte(length)

	if length < -120 {
		length = -(length + 120)
	} else {
		length = -(length + 112)
	}

	b := make([]byte, length + 1)
	b[0] = lengthByte
	for i := 1; i <= length; i++ {
	    b[i] = byte(n >> uint(8 * (length - i)))
	}
	return w.Write(b)
}
