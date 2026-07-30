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
	databasesMap  *component.SyncMap[string, *component.SyncMap[string, string]] // key = dbName
	dbFileDao     *dao.DBFileDao
	initComponent *component.InitComponent
}

func NewStoreLogic(ctx context.Context, initComponent *component.InitComponent, dbFileDao *dao.DBFileDao) *StoreLogic {
	onceStoreLogic.Do(func() {
		storeLogic = &StoreLogic{
			initComponent: initComponent,
			databasesMap:  &component.SyncMap[string, *component.SyncMap[string, string]]{},
			dbFileDao:     dbFileDao,
		}
	})
	storeLogic.initComponent.RegisterInitCallback(func(ctx context.Context) error {
		return storeLogic.LoadDBs(ctx)
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
		value, err := queryFromDBFile(ctx, s.dbFileDao, dbName, key)
		if err != nil {
			slog.ErrorContext(ctx, "query from db file failed", slog.String("dbName", dbName), slog.String("key", key), slog.Any("error", err))
			return "", fmt.Errorf("query from db failed")
		}
		db.Store(key, value)
		return value, nil
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

func (s *StoreLogic) LoadDBs(ctx context.Context) error {
	slog.InfoContext(ctx, "loading all dbs begin")

	dbFiles, err := s.dbFileDao.ListDBFiles(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "list db files failed", slog.Any("error", err))
		return err
	}

	for _, dbFile := range dbFiles {
		databaseEntity, err := s.dbFileDao.LoadDBByDBFile(ctx, dbFile)
		s.databasesMap.Store(databaseEntity.Name, &component.SyncMap[string, string]{})
		if err != nil {
			slog.ErrorContext(ctx, "load db failed", slog.String("dbFile", dbFile), slog.Any("error", err))
			return err
		}
	}

	slog.InfoContext(ctx, "loading all dbs done")
	return nil
}

// private functions =================================================
func queryFromDBFile(ctx context.Context, dbFileDao *dao.DBFileDao, dbName string, key string) (value string, err error) {
	return dbFileDao.ReadKey(ctx, dbName, key)
}
