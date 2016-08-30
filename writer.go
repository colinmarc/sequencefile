package sequencefile

import "io"

type Writer struct {
	Header Header
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(bytes []byte) (int, error) {
	return w.writer.Write(bytes)
}
