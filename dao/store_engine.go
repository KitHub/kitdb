package dao

import (
	"context"

	"github.com/KitHub/kitdb/component"
)

type StoreEngine interface {
	New(ctx context.Context, dataDir string, initCallbackComponent *component.InitComponent, shutdownCallbackComponent *component.ShutdownComponent) StoreEngine
	CreateDB(ctx context.Context, db string) error
	ReadKey(ctx context.Context, key string) (value string, err error)
	WriteKeyValue(ctx context.Context, db string, key string, value string) error
	Close(ctx context.Context) error
}
