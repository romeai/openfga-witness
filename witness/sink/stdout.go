package sink

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/openfga/openfga/witness/ocsf"
)

type StdoutSink struct {
	mu     sync.Mutex
	writer io.Writer
}

func NewStdoutSink(w io.Writer) *StdoutSink {
	return &StdoutSink{writer: w}
}

func (s *StdoutSink) Emit(_ context.Context, event ocsf.APIActivityEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.writer.Write(data)
	return err
}

func (s *StdoutSink) Close(_ context.Context) error {
	return nil
}
