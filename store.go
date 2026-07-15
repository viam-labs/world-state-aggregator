package wsaggregator

import (
	"sync"

	commonpb "go.viam.com/api/common/v1"
)

type store struct {
	mu         sync.RWMutex
	transforms map[string]*commonpb.Transform
}

func newStore() *store {
	return &store{transforms: make(map[string]*commonpb.Transform)}
}

func (s *store) list() [][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([][]byte, 0, len(s.transforms))
	for k := range s.transforms {
		out = append(out, []byte(k))
	}
	return out
}

func (s *store) get(uuid string) *commonpb.Transform {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.transforms[uuid]
}

func (s *store) set(uuid string, t *commonpb.Transform) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transforms[uuid] = t
}

func (s *store) remove(uuid string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.transforms, uuid)
}

func (s *store) snapshot() []*commonpb.Transform {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*commonpb.Transform, 0, len(s.transforms))
	for _, t := range s.transforms {
		out = append(out, t)
	}
	return out
}
