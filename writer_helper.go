package sequencefile

import (
	"encoding/binary"
	"io"
)

const syncMarker = -1

// A Writer wrapper that:
// - Stores any error that occurs, and stops writing.
// - Keeps track of how many bytes have been written.
// - Closes the wrapped writer, if it's close-able.
type writerHelper struct {
	w     io.Writer
	bytes int
	err   error
}

// Explicitly set an error on the helper. This is important if partial data may
// have been written, which should cause subsequent writes to return errors.
func (w *writerHelper) setErr(err error) {
	if w.err == nil {
		w.err = err
	}
}

func (w *writerHelper) Write(buf []byte) (n int, err error) {
	if w.err == nil {
		n, w.err = w.w.Write(buf)
		w.bytes += n
	}
	return n, w.err
}

func (w *writerHelper) Close() error {
	cw, ok := w.w.(io.WriteCloser)
	if ok {
		return cw.Close()
	}
	return nil
}

func (w *writerHelper) write(buf []byte) error {
	_, err := w.Write(buf)
	return err
}

func (w *writerHelper) writeString(s string) error {
	WriteVInt(w, int64(len(s)))
	return w.write([]byte(s))
}

func (w *writerHelper) writeBool(b bool) error {
	if b {
		return w.write([]byte{1})
	}
	return w.write([]byte{0})
}

func (w *writerHelper) writeInt32(i int32) error {
	var bs [4]byte
	binary.BigEndian.PutUint32(bs[:], uint32(i))
	return w.write(bs[:])
}

func (w *writerHelper) writeSync(sync []byte) error {
	w.writeInt32(syncMarker)
	w.write(sync)
	w.bytes = 0
	return w.err
}
