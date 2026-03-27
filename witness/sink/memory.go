package sink

import (
	"context"
	"sync"

	"github.com/openfga/openfga/witness/ocsf"
)

type MemorySink struct {
	mu     sync.Mutex
	events []ocsf.APIActivityEvent
}

func NewMemorySink() *MemorySink {
	return &MemorySink{}
}

func (s *MemorySink) Emit(_ context.Context, event ocsf.APIActivityEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *MemorySink) Close(_ context.Context) error {
	return nil
}

func (s *MemorySink) Events() []ocsf.APIActivityEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ocsf.APIActivityEvent, len(s.events))
	copy(out, s.events)
	return out
}

func (s *MemorySink) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = nil
}
