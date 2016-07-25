Sequencefile
============

[![GoDoc](https://godoc.org/github.com/colinmarc/sequencefile/web?status.svg)](https://godoc.org/github.com/colinmarc/sequencefile) [![build](https://travis-ci.org/colinmarc/sequencefile.svg?branch=master)](https://travis-ci.org/colinmarc/sequencefile)

This is a native Go implementation of [Hadoop's SequenceFile format][1].

[1]: https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/SequenceFile.html

Usage
-----

```go
sf, err := sequencefile.Open("foo.sequencefile")
if err != nil {
  log.Fatal(err)
}

// Iterate through the file.
for sf.Scan() {
  // Do something with sf.Key() and sf.Value()
}

if sf.Err() != nil {
  log.Fatal(err)
}
```

Reading files written by Hadoop
-------------------------------

Hadoop adds another layer of serialization for individual keys and values,
depending on the class used, like [BytesWritable][2]. By default, this library
will return the raw key and value bytes, still serialized. You can use the
following methods to unwrap them:

```
func BytesWritable(b []byte) []byte
func Text(b []byte) string
func IntWritable(b []byte) int32
func LongWritable(b []byte) int64
```

[2]: https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/io/BytesWritable.html
