package dao

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/KitHub/kitdb/component"
	"github.com/KitHub/kitdb/entity"
)

const (
	dbFileSuffix = ".db"
)

var dbFileDao *DBFileDao
var onceForDBFileDao sync.Once

type DBFileDao struct {
	dataDir      string
	databasesMap *component.SyncMap[string, *entity.DatabaseEntity] // key=dbname
}

func NewDBFileDao(ctx context.Context, dataDir string) *DBFileDao {
	onceForDBFileDao.Do(func() {
		dbFileDao = &DBFileDao{
			dataDir:      dataDir,
			databasesMap: &component.SyncMap[string, *entity.DatabaseEntity]{},
		}
	})
	return dbFileDao
}

func (d *DBFileDao) CreateDBFile(ctx context.Context, dbName string) error {
	dbEntity, ok := d.databasesMap.Load(dbName)
	if !ok {
		dbEntity = &entity.DatabaseEntity{
			Name:   dbName,
			DBFile: nil,
		}
		d.databasesMap.Store(dbName, dbEntity)
	}
	dbFileName := dbName + "." + dbFileSuffix
	dbFile, err := createFileInFolder(d.dataDir, dbFileName, false)
	if err != nil {
		slog.ErrorContext(ctx, "create db file failed", slog.String("dataDir", d.dataDir), slog.String("dbFileName", dbFileName), slog.Any("error", err))
		return err
	}
	dbEntity.DBFile = dbFile

	err = initDBFile(ctx, dbEntity)
	if err != nil {
		slog.ErrorContext(ctx, "init db file failed", slog.String("dbFile", dbEntity.Name), slog.Any("error", err))
		return err
	}

	return nil
}

// private functions ================================================================

// createFileInFolder
func createFileInFolder(folder string, filename string, append bool) (*os.File, error) {
	fullPath := filepath.Join(folder, filename)
	var flag int
	if append {
		flag = os.O_CREATE | os.O_WRONLY | os.O_APPEND
	} else {
		flag = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	}

	return os.OpenFile(fullPath, flag, 0644)
}

func initDBFile(ctx context.Context, dbEntity *entity.DatabaseEntity) error {
	_, err := dbEntity.DBFile.WriteString("db:" + dbEntity.Name + ";timestamp:" + fmt.Sprintf("%d", time.Now().UnixMilli()))
	if err != nil {
		slog.ErrorContext(ctx, "write db file header failed", slog.String("dbFile", dbEntity.Name), slog.Any("error", err))
		return err
	}
	return nil
}
