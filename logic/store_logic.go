package logic

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/KitHub/kitdb/component"
	"github.com/KitHub/kitdb/dao"
)

var storeLogic *StoreLogic
var onceStoreLogic sync.Once

type StoreLogic struct {
	databasesMap *component.SyncMap[string, *component.SyncMap[string, string]] // key = dbName
	dbFileDao    *dao.DBFileDao
}

func NewStoreLogic(ctx context.Context, dbFileDao *dao.DBFileDao) *StoreLogic {
	onceStoreLogic.Do(func() {
		storeLogic = &StoreLogic{
			databasesMap: &component.SyncMap[string, *component.SyncMap[string, string]]{},
			dbFileDao:    dbFileDao,
		}
	})
	return storeLogic
}

func (s *StoreLogic) ReadKey(ctx context.Context, dbName string, key string) (string, error) {
	slog.DebugContext(ctx, "read key", slog.String("db", dbName), slog.String("key", key))
	db, ok := s.databasesMap.Load(dbName)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("dbName", dbName))
		return "", fmt.Errorf("db not found: %s", dbName)
	}

	value, ok := db.Load(key)
	if !ok {
		slog.ErrorContext(ctx, "key not found", slog.String("dbName", dbName), slog.String("key", key))
		return "", fmt.Errorf("key not found: %s", key)
	}

	slog.DebugContext(ctx, "read key done", slog.String("db", dbName), slog.String("key", key), slog.String("value", value))
	return value, nil
}

func (s *StoreLogic) WriteKeyValue(ctx context.Context, dbName string, key string, value string) error {
	db, ok := s.databasesMap.Load(dbName)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("dbName", dbName))
		return fmt.Errorf("db not found: %s", dbName)
	}

	err := s.dbFileDao.WriteKeyValue(ctx, dbName, key, value)
	if err != nil {
		slog.ErrorContext(ctx, "write data log failed", slog.String("dbName", dbName), slog.String("key", key), slog.String("value", value))
		return err
	}
	db.Store(key, value)

	slog.DebugContext(ctx, "read key done", slog.String("db", dbName), slog.String("key", key), slog.String("value", value))
	return nil
}

func (s *StoreLogic) CreateDB(ctx context.Context, dbName string) error {
	slog.DebugContext(ctx, "create db", slog.String("db", dbName))
	_, ok := s.databasesMap.Load(dbName)
	if ok {
		slog.ErrorContext(ctx, "db already existed", slog.String("dbName", dbName))
		return fmt.Errorf("db already existed: %s", dbName)
	}

	err := s.dbFileDao.CreateDBFile(ctx, dbName)
	if err != nil {
		slog.ErrorContext(ctx, "create db file failed", slog.String("db", dbName), slog.Any("error", err))
		return fmt.Errorf("create db file failed: %s", dbName)
	}

	db := &component.SyncMap[string, string]{}
	s.databasesMap.Store(dbName, db)

	slog.DebugContext(ctx, "create db done", slog.String("db", dbName))
	return nil
}
