package component

import (
	"context"
	"log/slog"
	"sync"
)

var initComponentInstance *InitComponent
var onceForInitComponentInstance sync.Once = sync.Once{}

type InitCallback func(ctx context.Context) error

type InitComponent struct {
	initCallbacks      []InitCallback
	lock               *sync.Mutex
	initCallbacksDoing bool
}

func (i *InitComponent) ExecuteInitdownCallbacks(ctx context.Context) error {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.initCallbacksDoing {
		slog.WarnContext(ctx, "init callbacks are already being executed, skipping duplicate execution")
		return nil
	}
	i.initCallbacksDoing = true
	defer func() {
		i.initCallbacksDoing = false
	}()

	var err error
	for _, callback := range i.initCallbacks {
		if err = callback(ctx); err != nil {
			slog.ErrorContext(ctx, "execute init callback failed", slog.Any("error", err), slog.Any("callback", callback))
			// continue to execute the remaining callbacks even if one fails
		}
	}

	return err
}

func (i *InitComponent) RegisterInitCallback(callback InitCallback) {
	i.lock.Lock()
	defer i.lock.Unlock()

	if i.initCallbacksDoing {
		slog.Warn("init callbacks are already being executed, cannot register new callback", slog.Any("callback", callback))
		return
	}

	i.initCallbacks = append(i.initCallbacks, callback)
}

func (i *InitComponent) GetInitCallbacks(ctx context.Context) []InitCallback {
	i.lock.Lock()
	defer i.lock.Unlock()
	return i.initCallbacks
}

func NewInitComponent(ctx context.Context) *InitComponent {
	onceForInitComponentInstance.Do(func() {
		initComponentInstance = &InitComponent{
			initCallbacks: make([]InitCallback, 0),
			lock:          &sync.Mutex{},
		}
	})
	return initComponentInstance
}
