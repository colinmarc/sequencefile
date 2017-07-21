package sequencefile

import "bytes"

const (
	syncBytes        = 2000
	defaultBlockSize = 1000 * 1000
)

type pairWriter interface {
	Init() error
	Write(key, value []byte) error
	Close() error
}

type uncompressedPairs struct {
	w    *writerHelper
	sync []byte
}

func (p *uncompressedPairs) Write(key, value []byte) error {
	if p.w.bytes > syncBytes {
		p.w.writeSync(p.sync)
	}

	p.w.writeInt32(int32(len(key) + len(value)))
	p.w.writeInt32(int32(len(key)))
	p.w.write(key)
	p.w.write(value)
	return p.w.err
}

func (p *uncompressedPairs) Init() error {
	return nil
}

func (p *uncompressedPairs) Close() error {
	return nil
}

type recordCompressedPairs struct {
	uncompressedPairs
	compressor compressor
}

func (p *recordCompressedPairs) Write(key, value []byte) (err error) {
	value, err = p.compressor.compress(value)
	if err != nil {
		return err
	}
	return p.uncompressedPairs.Write(key, value)
}

type blockPairs struct {
	w          *writerHelper
	sync       []byte
	compressor compressor
	blockSize  int

	keys         []byte
	keyLengths   []int
	values       []byte
	valueLengths []int
}

func (b *blockPairs) writeCompressed(buf []byte) error {
	c, err := b.compressor.compress(buf)
	if err != nil {
		b.w.setErr(err)
		return err
	}
	WriteVInt(b.w, int64(len(c)))
	return b.w.write(c)
}

func (b *blockPairs) writeLengths(lengths []int) error {
	var buf bytes.Buffer
	for _, l := range lengths {
		if err := WriteVInt(&buf, int64(l)); err != nil {
			b.w.setErr(err)
			return err
		}
	}
	return b.writeCompressed(buf.Bytes())
}

func (b *blockPairs) writeBlock() (err error) {
	b.w.writeSync(b.sync)

	count := len(b.keyLengths)
	WriteVInt(b.w, int64(count))

	b.writeLengths(b.keyLengths)
	b.writeCompressed(b.keys)
	b.writeLengths(b.valueLengths)
	b.writeCompressed(b.values)

	b.keys = nil
	b.keyLengths = nil
	b.values = nil
	b.valueLengths = nil
	return b.w.err
}

func (b *blockPairs) Write(key, value []byte) error {
	b.keys = append(b.keys, key...)
	b.keyLengths = append(b.keyLengths, len(key))
	b.values = append(b.values, value...)
	b.valueLengths = append(b.valueLengths, len(value))
	if len(b.keys)+len(b.values) >= b.blockSize {
		return b.writeBlock()
	}
	return nil
}

func (b *blockPairs) Init() error {
	if b.blockSize == 0 {
		b.blockSize = defaultBlockSize
	}
	return nil
}

func (b *blockPairs) Close() error {
	if len(b.keyLengths) > 0 {
		return b.writeBlock()
	}
	return nil
}
