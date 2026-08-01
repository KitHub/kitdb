package dao

import (
	"context"

	"github.com/KitHub/protocols/kitdb"
)

type StorageEngine interface {
	CreateDB(ctx context.Context, db string) error
	CreateIndex(ctx context.Context, db string, index string, indexType kitdb.IndexType, fields []string) error
	ReadKey(ctx context.Context, db string, key string) (value string, ok bool, err error)
	WriteKeyValue(ctx context.Context, db string, key string, value string) error
	Close(ctx context.Context) error
	InitExistedDBs(ctx context.Context) error
	GetDBNames(ctx context.Context) ([]string, error)
}
