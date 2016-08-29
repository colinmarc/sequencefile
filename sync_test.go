package sequencefile

import (
	"os"
	"testing"
)

func TestSync(t *testing.T) {
	file, err := os.Open("../test-huge/20150826/part-00000")

	// We need a big file for this one.
	if os.IsNotExist(err) {
		t.Skip()
	} else if err != nil {
		require.NoError(t, err)
	}

	r := New(file)
	r.ReadHeader()

	rand.Seed(time.Now().Unix())
	for i := 0; i < 1000; i++ {
		offset := int64(rand.Intn(1000000000)) + 1000
		file.Seek(offset, os.SEEK_SET)
		err = r.Sync()
		require.NoError(t, err)

		ok := r.Scan()
		require.True(t, ok)
		require.Nil(t, r.Err())
	}
}
