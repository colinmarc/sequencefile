package sequencefile

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
)

const maxSyncRead = 104857600 // 100mb
var ErrNoSync = fmt.Errorf("Couldn't find a valid sync marker within %d bytes", maxSyncRead)

// Sync reads forward in the file until it finds the next sync marker. After
// calling Sync, the Reader will be placed right before the next record or
// block. This allows you to seek the underlying Reader to a random offset, and
// then realign with a record.
//
// Sync makes many Read calls to the underlying reader to ensure that it doesn't
// read more than it needs to. It's a good idea to construct the reader with a
// bufio.Reader if you value performance, but don't care about this property.
func (r *Reader) Sync() error {
	var read int64
	readNext := SyncSize
	buf := new(bytes.Buffer)

	for read < maxSyncRead {
		buf.Grow(readNext)
		n, err := io.CopyN(buf, r.reader, int64(readNext))
		read += n
		if err != nil {
			return err
		}

		// Try to find part of the sync marker in the buffer we read. If we find any
		// part of it that matches, pretend it is the beginning of it and only read
		// enough to get the whole thing. That way, we never read too much.
		//
		// This method is heavy on read calls, but ensures that the underlying
		// reader always has the correct offset.
		found := false
		b := buf.Bytes()
		for off := 0; off < SyncSize; off++ {
			if bytes.Compare(b[off:], r.syncMarkerBytes[:(SyncSize-off)]) == 0 {
				if off == 0 {
					// Found it!
					return nil
				}

				// Found what looks like a chunk of it. Cycle the buffer forward, and
				// prepare to read what should be the rest of the sync marker.
				found = true
				io.CopyN(ioutil.Discard, buf, int64(off))
				readNext = off
				break
			}
		}

		if !found {
			buf.Reset()
			readNext = SyncSize
		}
	}

	return ErrNoSync
}
