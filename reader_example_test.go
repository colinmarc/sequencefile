package sequencefile_test

import (
	"fmt"
	"log"

	"github.com/colinmarc/sequencefile"
)

func ExampleReader() {
	sf, err := sequencefile.Open("testdata/block_compressed_snappy.sequencefile")
	if err != nil {
		log.Fatal(err)
	}

	// Iterate through the file.
	for sf.Scan() {
		// Unwrap the BytesWritable values.
		key := sequencefile.BytesWritable(sf.Key())
		value := sequencefile.BytesWritable(sf.Value())
		fmt.Println(string(key), string(value))
	}

	if sf.Err() != nil {
		log.Fatal(err)
	}

	// Output:
	// Alice Practice
	// Bob Hope
}
