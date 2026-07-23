package component

import (
	"context"
	"log/slog"
	"sync"
)

var shutdownComponentInstance *ShutdownComponent
var onceForShutdownComponentInstance sync.Once = sync.Once{}

type ShutdownCallback func(ctx context.Context) error

type ShutdownComponent struct {
	shutdownCallbacks      []ShutdownCallback
	lock                   *sync.Mutex
	shutdownCallbacksDoing bool
}

func (s *ShutdownComponent) ExecuteShutdownCallbacks(ctx context.Context) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.shutdownCallbacksDoing {
		slog.WarnContext(ctx, "shutdown callbacks are already being executed, skipping duplicate execution")
		return nil
	}
	s.shutdownCallbacksDoing = true
	defer func() {
		s.shutdownCallbacksDoing = false
	}()

	var err error
	for _, callback := range s.shutdownCallbacks {
		if err = callback(ctx); err != nil {
			slog.ErrorContext(ctx, "execute shutdown callback failed", slog.Any("error", err), slog.Any("callback", callback))
			// continue to execute the remaining callbacks even if one fails
		}
	}

	return err
}

func (s *ShutdownComponent) RegisterShutdownCallback(callback ShutdownCallback) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if s.shutdownCallbacksDoing {
		slog.Warn("shutdown callbacks are already being executed, cannot register new callback", slog.Any("callback", callback))
		return
	}

	s.shutdownCallbacks = append(s.shutdownCallbacks, callback)
}

func (s *ShutdownComponent) GetShutdownCallbacks(ctx context.Context) []ShutdownCallback {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.shutdownCallbacks
}

func NewShutdownComponent(ctx context.Context) *ShutdownComponent {
	onceForShutdownComponentInstance.Do(func() {
		shutdownComponentInstance = &ShutdownComponent{
			shutdownCallbacks: make([]ShutdownCallback, 0),
			lock:              &sync.Mutex{},
		}
	})
	return shutdownComponentInstance
}
