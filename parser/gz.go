package parser

import (
	"compress/gzip"
	"io"
	"os"
)

func getReader(path string) (*gzip.Reader, []io.Closer, error) {
	osf, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	gzr, err := gzip.NewReader(osf)
	if err != nil {
		return nil, []io.Closer{osf}, err
	}

	return gzr, []io.Closer{gzr, osf}, nil
}
