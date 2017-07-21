package sequencefile_test

import (
	"bytes"
	"log"

	"github.com/colinmarc/sequencefile"
)

func ExampleWriter() {
	var buf bytes.Buffer

	cfg := &sequencefile.WriterConfig{
		Writer:     &buf,
		KeyClass:   sequencefile.BytesWritableClassName,
		ValueClass: sequencefile.BytesWritableClassName,
	}
	w, err := sequencefile.NewWriter(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	pairs := []struct{ k, v string }{
		{"Alice", "Practice"},
		{"Bob", "Hope"},
	}
	for _, p := range pairs {
		err = w.Append([]byte(p.k), []byte(p.v))
		if err != nil {
			log.Fatal(err)
		}
	}
}
