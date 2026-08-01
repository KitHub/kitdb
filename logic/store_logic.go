package logic

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/KitHub/kitdb/component"
	"github.com/KitHub/kitdb/dao"
	"github.com/KitHub/protocols/kitdb"
)

var storeLogic *StoreLogic
var onceStoreLogic sync.Once

type StoreLogic struct {
	dataCaches    *component.SyncMap[string, *component.SyncMap[string, string]] // key = dbName
	storageEngine dao.StorageEngine
	initComponent *component.InitComponent
}

func NewStoreLogic(ctx context.Context, initComponent *component.InitComponent, storageEngine dao.StorageEngine) *StoreLogic {
	onceStoreLogic.Do(func() {
		storeLogic = &StoreLogic{
			initComponent: initComponent,
			dataCaches:    &component.SyncMap[string, *component.SyncMap[string, string]]{},
			storageEngine: storageEngine,
		}
	})
	storeLogic.initComponent.RegisterInitCallback(func(ctx context.Context) error {
		return storeLogic.InitDBs(ctx)
	})
	return storeLogic
}

func (s *StoreLogic) ReadKey(ctx context.Context, dbName string, key string) (string, bool, error) {
	slog.DebugContext(ctx, "read key", slog.String("db", dbName), slog.String("key", key))
	dbCache, ok := s.dataCaches.Load(dbName)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("dbName", dbName))
		return "", false, fmt.Errorf("db not found: %s", dbName)
	}

	value, ok := dbCache.Load(key)
	if !ok {
		value, ok, err := queryFromStorageEngine(ctx, s.storageEngine, dbName, key)
		if err != nil {
			slog.ErrorContext(ctx, "query from db file failed", slog.String("dbName", dbName), slog.String("key", key), slog.Any("error", err))
			return "", false, fmt.Errorf("query from db failed")
		}
		if !ok {
			return "", false, nil
		}
		dbCache.Store(key, value)
		return value, true, nil
	}

	slog.DebugContext(ctx, "read key done", slog.String("db", dbName), slog.String("key", key), slog.String("value", value))
	return value, true, nil
}

func (s *StoreLogic) WriteKeyValue(ctx context.Context, dbName string, key string, value string) error {
	dbCache, ok := s.dataCaches.Load(dbName)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("dbName", dbName))
		return fmt.Errorf("db not found: %s", dbName)
	}

	err := s.storageEngine.WriteKeyValue(ctx, dbName, key, value)
	if err != nil {
		slog.ErrorContext(ctx, "write data log failed", slog.String("dbName", dbName), slog.String("key", key), slog.String("value", value))
		return err
	}
	dbCache.Store(key, value)

	slog.DebugContext(ctx, "write key value done", slog.String("db", dbName), slog.String("key", key), slog.String("value", value))
	return nil
}

func (s *StoreLogic) CreateDB(ctx context.Context, dbName string) error {
	slog.InfoContext(ctx, "create db", slog.String("db", dbName))
	_, ok := s.dataCaches.Load(dbName)
	if ok {
		slog.ErrorContext(ctx, "db already existed", slog.String("dbName", dbName))
		return fmt.Errorf("db already existed: %s", dbName)
	}

	err := s.storageEngine.CreateDB(ctx, dbName)
	if err != nil {
		slog.ErrorContext(ctx, "create db file failed", slog.String("db", dbName), slog.Any("error", err))
		return fmt.Errorf("create db file failed: %s", dbName)
	}

	db := &component.SyncMap[string, string]{}
	s.dataCaches.Store(dbName, db)

	slog.DebugContext(ctx, "create db done", slog.String("db", dbName))
	return nil
}

func (s *StoreLogic) CreateIndex(ctx context.Context, dbName string, indexName string, indexType kitdb.IndexType, fields []string) error {
	return s.storageEngine.CreateIndex(ctx, dbName, indexName, indexType, fields)
}

func (s *StoreLogic) InitDBs(ctx context.Context) error {
	slog.InfoContext(ctx, "init all dbs begin")

	dbNames, err := s.storageEngine.GetDBNames(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "get db names failed", slog.Any("error", err))
		return err
	}

	for _, dbName := range dbNames {
		s.dataCaches.Store(dbName, &component.SyncMap[string, string]{})
	}

	slog.InfoContext(ctx, "init all dbs done")
	return nil
}

// private functions =================================================
func queryFromStorageEngine(ctx context.Context, storageEngine dao.StorageEngine, dbName string, key string) (value string, ok bool, err error) {
	return storageEngine.ReadKey(ctx, dbName, key)
}
