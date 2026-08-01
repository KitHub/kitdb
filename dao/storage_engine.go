package dao

import (
	"context"
)

type StorageEngine interface {
	CreateDB(ctx context.Context, db string) error
	ReadKey(ctx context.Context, db string, key string) (value string, err error)
	WriteKeyValue(ctx context.Context, db string, key string, value string) error
	Close(ctx context.Context) error
	InitExistedDBs(ctx context.Context) error
	GetDBNames(ctx context.Context) ([]string, error)
}
