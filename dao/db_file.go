package dao

import (
	"context"
	"sync"
)

var dbFileDao *DBFileDao
var onceForDBFileDao sync.Once

type DBFileDao struct {
	dataDir string
}

func NewDBFileDao(ctx context.Context, dataDir string) *DBFileDao {
	onceForDBFileDao.Do(func() {
		dbFileDao = &DBFileDao{
			dataDir: dataDir,
		}
	})
	return dbFileDao
}

func (d *DBFileDao) CreateDBFile(ctx context.Context, dbName string) string {
	return ""
}
