package sequencefile

// BytesWritable unwraps a BytesWritable and returns the actual bytes.
func BytesWritable(b []byte) []byte {
	return b[4:]
}
