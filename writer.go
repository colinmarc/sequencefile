package sequencefile

import "io"

type Writer struct {
	Header Header
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	header := Header{
		Version:        6,
		KeyClassName:   "org.apache.hadoop.io.BytesWritable",
		ValueClassName: "org.apache.hadoop.io.BytesWritable",
		Compression:    NoCompression,
	}
	return &Writer{writer: w, Header: header}
}

func (w *Writer) Write(bytes []byte) (int, error) {
	return w.writer.Write(bytes)
}
