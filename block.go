package sequencefile

import (
	"bytes"
	"errors"
	"io"
)

// a blockReader represents an iterator over a single compressed block, for
// block-compressed SequenceFiles.
type blockReader struct {
	n     int
	i     int
	key   []byte
	value []byte
	err   error

	keys       []byte
	keyLengths []int
	keyOffset  int

	values       []byte
	valueLengths []int
	valueOffset  int
}

func (b *blockReader) next() bool {
	if b.i >= b.n {
		return false
	}

	keyLength := b.keyLengths[b.i]
	b.key = b.keys[b.keyOffset : b.keyOffset+keyLength]
	b.keyOffset += keyLength

	if b.values != nil {
		valueLength := b.valueLengths[b.i]
		b.value = b.values[b.valueOffset : b.valueOffset+valueLength]
		b.valueOffset += valueLength
	}

	b.i++
	return true
}

func (r *Reader) scanBlock() bool {
	for !r.block.next() {
		err := r.startBlock()
		if err == io.EOF {
			return false
		} else if err != nil {
			r.close(err)
			return false
		}
	}

	r.key = r.block.key
	r.value = r.block.value
	return true
}

func (r *Reader) startBlock() error {
	// The sync appears at the start of every block, but it still has the -1
	// length prefix in front, just for funsies.
	r.clear()
	_, err := r.consume(4)
	if err != nil {
		return err
	}

	err = r.checkSync()
	if err != nil {
		return err
	}

	n, err := ReadVInt(r.reader)
	if err != nil {
		return err
	}

	block := blockReader{n: int(n)}
	r.clear()

	keyLengthsBytes, err := r.consumeSection()
	if err != nil {
		return err
	}

	block.keyLengths, err = readLengths(keyLengthsBytes, int(n))
	if err != nil {
		return err
	}

	block.keys, err = r.consumeSection()
	if err != nil {
		return err
	}

	valueLengthsBytes, err := r.consumeSection()
	if err != nil {
		return err
	}

	block.valueLengths, err = readLengths(valueLengthsBytes, int(n))
	if err != nil {
		return err
	}

	block.values, err = r.consumeSection()
	if err != nil {
		return err
	}

	r.block = block
	return nil
}

func (r *Reader) consumeSection() ([]byte, error) {
	length, err := ReadVInt(r.reader)
	if err != nil {
		return nil, err
	}

	return r.consumeCompressed(int(length))
}

func readLengths(b []byte, n int) ([]int, error) {
	buf := bytes.NewBuffer(b)
	res := make([]int, 0, n)

	for i := 0; i < n; i++ {
		vint, err := ReadVInt(buf)
		if err == io.EOF {
			return nil, io.ErrUnexpectedEOF
		} else if err != nil {
			return nil, err
		}

		res = append(res, int(vint))
	}

	if buf.Len() != 0 {
		return nil, errors.New("sequencefile: invalid lengths for block")
	}

	return res, nil
}

type blockWriter struct {
	keys         [][]byte
	keyLengths   []int64
	keysLength   int64
	values       [][]byte
	valueLengths []int64
	valuesLength int64
}

func (bw *blockWriter) Append(key []byte, value []byte) {
	bw.keys = append(bw.keys, key)
	bw.keyLengths = append(bw.keyLengths, int64(len(key)))
	bw.keysLength += int64(len(key))
	bw.values = append(bw.values, value)
	bw.valueLengths = append(bw.valueLengths, int64(len(value)))
	bw.valuesLength += int64(len(value))
}

func (bw *blockWriter) FlushBlock(w *Writer) (int, error) {
	var err error
	var written int
	var totalWritten int
	if len(bw.keys) == 0 {
		return 0, nil
	}
	written, err = w.sync()
	if err != nil {
		return written, err
	}

	// write out the number of key/value pairs in this block
	written, err = WriteVInt(w.writer, int64(len(bw.keys)))
	totalWritten += written
	if err != nil {
		return totalWritten, err
	}

	// key lengths block
	keyLengthsBytes := make([]byte, 0)
	for _, keyLength := range bw.keyLengths {
		keyLengthsBytes = append(keyLengthsBytes, PutVInt(keyLength)...)
	}
	keyLengthsBytesCompressed, err := w.compress(keyLengthsBytes)
	if err != nil {
		return totalWritten, err
	}

	written, err = WriteVInt(w.writer, int64(len(keyLengthsBytesCompressed)))
	totalWritten += written
	if err != nil {
		return totalWritten, err
	}
	written, err = w.writer.Write(keyLengthsBytesCompressed)
	totalWritten += written
	if err != nil {
		return totalWritten, err
	}

	// keys block
	keysBytes := make([]byte, 0)
	for _, key := range bw.keys {
		keysBytes = append(keysBytes, key...)
	}
	keysBytesCompressed, err := w.compress(keysBytes)
	if err != nil {
		return totalWritten, err
	}

	written, err = WriteVInt(w.writer, int64(len(keysBytesCompressed)))
	totalWritten += written
	if err != nil {
		return totalWritten, err
	}
	written, err = w.writer.Write(keysBytesCompressed)
	totalWritten += written
	if err != nil {
		return totalWritten, err
	}

	// value length block
	valueLengthsBytes := make([]byte, 0)
	for _, valueLength := range bw.valueLengths {
		valueLengthsBytes = append(valueLengthsBytes, PutVInt(valueLength)...)
	}
	valueLengthsBytesCompressed, err := w.compress(valueLengthsBytes)
	if err != nil {
		return totalWritten, err
	}

	written, err = WriteVInt(w.writer, int64(len(valueLengthsBytesCompressed)))
	totalWritten += written
	if err != nil {
		return totalWritten, err
	}
	written, err = w.writer.Write(valueLengthsBytesCompressed)
	totalWritten += written
	if err != nil {
		return totalWritten, err
	}

	// values block
	valuesBytes := make([]byte, 0)
	for _, value := range bw.values {
		valuesBytes = append(valuesBytes, value...)
	}
	valuesBytesCompressed, err := w.compress(valuesBytes)
	if err != nil {
		return totalWritten, err
	}

	written, err = WriteVInt(w.writer, int64(len(valuesBytesCompressed)))
	totalWritten += written
	if err != nil {
		return totalWritten, err
	}
	written, err = w.writer.Write(valuesBytesCompressed)
	totalWritten += written
	if err != nil {
		return totalWritten, err
	}

	bw.Reset()
	return totalWritten, nil
}

func (bw *blockWriter) Reset() {
	bw.keys = make([][]byte, 0)
	bw.keyLengths = make([]int64, 0)
	bw.keysLength = 0
	bw.values = make([][]byte, 0)
	bw.valueLengths = make([]int64, 0)
	bw.valuesLength = 0
}
