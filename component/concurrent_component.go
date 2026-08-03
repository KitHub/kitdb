package component

import (
	"encoding/json"
	"sync"
)

type SyncMap[K comparable, V any] struct {
	m sync.Map
}

func (s *SyncMap[K, V]) Store(k K, v V) {
	s.m.Store(k, v)
}

func (s *SyncMap[K, V]) Load(k K) (V, bool) {
	val, ok := s.m.Load(k)
	if !ok {
		var zero V
		return zero, false
	}
	return val.(V), true
}

func (s *SyncMap[K, V]) Delete(k K) {
	s.m.Delete(k)
}

func (s *SyncMap[K, V]) Keys() []K {
	var keys []K
	s.m.Range(func(key, value any) bool {
		keys = append(keys, key.(K))
		return true
	})
	return keys
}

func (s *SyncMap[K, V]) Range(f func(key K, value V) bool) {
	s.m.Range(func(key, value any) bool {
		return f(key.(K), value.(V))
	})
}

func (s *SyncMap[K, V]) MarshalJSON() ([]byte, error) {
	tmpMap := make(map[K]V)
	s.Range(func(key K, value V) bool {
		tmpMap[key] = value
		return true
	})
	return json.Marshal(tmpMap)
}

func (s *SyncMap[K, V]) UnmarshalJSON(data []byte) error {
	tmpMap := make(map[K]V)
	if err := json.Unmarshal(data, &tmpMap); err != nil {
		return err
	}
	for key, value := range tmpMap {
		s.Store(key, value)
	}
	return nil
}
