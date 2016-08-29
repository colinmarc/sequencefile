package sequencefile

import "testing"
import "bytes"
import "github.com/stretchr/testify/assert"

func TestReadBool(t *testing.T) {
	buf := new(bytes.Buffer)
	buf.Write([]byte{0x00})
	r := NewReader(buf)
	ret, err := r.readBoolean()
	assert.Equal(t, false, ret, "readBoolean should return a false value")
	assert.NoError(t, err, "readBoolean should be successful")

	buf.Write([]byte{0x01})

	ret, err = r.readBoolean()
	assert.Equal(t, true, ret, "readBoolean should return a true value")
	assert.NoError(t, err, "readBoolean should be successful")
}

func TestWriteBool(t *testing.T) {
	buf := new(bytes.Buffer)
	w := NewWriter(buf)
	num, err := w.writeBoolean(false)
	assert.Equal(t, []byte{0x00}, buf.Bytes(), "writeBoolean should write a false value")
	assert.Equal(t, 1, num, "writeBoolean should write one byte")
	assert.NoError(t, err, "writeBoolean should be successful")

	buf = new(bytes.Buffer)
	w = NewWriter(buf)
	num, err = w.writeBoolean(true)
	assert.Equal(t, []byte{0x01}, buf.Bytes(), "writeBoolean should write a true value")
	assert.Equal(t, 1, num, "writeBoolean should write one byte")
	assert.NoError(t, err, "writeBoolean should be successful")
}
