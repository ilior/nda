// productsImporter project fileReader.go
package fileReader

import (
	"errors"

	"github.com/pkg/sftp"

	"encoding/csv"

	"nda/productsImporter/internal/entity"
)

type options struct {
	filepath string
	storage  string
}

func Process(client *sftp.Client, filepath string, done <-chan struct{}) (<-chan entity.ParsedLine, <-chan error) {
	errc := make(chan error, 1)

	opt := options{filepath, "aparser"}

	source, err := client.Open(opt.filepath)

	if err != nil {
		errc <- err
		return nil, errc
	}

	r := csv.NewReader(source)

	_, err = r.Read() //skip titles

	if err != nil {
		errc <- err
		return nil, errc
	}

	headers, err := r.Read()

	if err != nil {
		errc <- err
		return nil, errc
	}

	out := make(chan entity.ParsedLine)
	//defer close(out)

	headersMap := make(map[string]int, len(headers))

	for i, v := range headers {
		headersMap[v] = i
	}

	go func() {
		defer source.Close()
		defer close(out)
		errc <- read(r, headersMap, out, done)
	}()

	return out, errc
}

func read(r *csv.Reader, headersMap map[string]int, out chan<- entity.ParsedLine, done <-chan struct{}) error {
	c := 0
	for {
		line, err := r.Read()

		c++

		if c > 10 {
			return errors.New("debug limit reached")
		}

		if err != nil {
			return err
		}

		data := make(entity.ParsedLine, len(headersMap))

		for v, i := range headersMap {
			data[v] = line[i]
		}

		select {
		case out <- data:
		case <-done:
			return errors.New("reading stopped")

		}
	}
	return nil
}
