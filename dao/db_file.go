package dao

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/KitHub/kitdb/component"
	"github.com/KitHub/kitdb/entity"
)

const (
	dbFileSuffix = ".db"
)

var lineBreak string
var dbFileDao *DBFileDao
var onceForDBFileDao sync.Once

type DBFileDao struct {
	dataDir           string
	databasesMap      *component.SyncMap[string, *entity.DatabaseEntity] // key=dbname
	shutdownComponent *component.ShutdownComponent
}

func NewDBFileDao(ctx context.Context, dataDir string, shutdownComponent *component.ShutdownComponent) *DBFileDao {
	onceForDBFileDao.Do(func() {
		if runtime.GOOS == "windows" {
			lineBreak = "\r\n"
		} else {
			lineBreak = "\n"
		}

		dbFileDao = &DBFileDao{
			dataDir:           dataDir,
			databasesMap:      &component.SyncMap[string, *entity.DatabaseEntity]{},
			shutdownComponent: shutdownComponent,
		}

		dbFileDao.shutdownComponent.RegisterShutdownCallback(func(ctx context.Context) error {
			dbNames := dbFileDao.databasesMap.Keys()
			for _, dbName := range dbNames {
				_ = dbFileDao.CloseDBFile(ctx, dbName)
			}
			return nil
		})
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

func (d *DBFileDao) AppendLine(ctx context.Context, dbName string, content string) error {
	dbEntity, ok := d.databasesMap.Load(dbName)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("dbName", dbName))
		return fmt.Errorf("db not found: %s", dbName)
	}
	_, err := dbEntity.DBFile.WriteString(content + lineBreak)
	if err != nil {
		slog.ErrorContext(ctx, "append db file failed", slog.String("dbName", dbName))
		return err
	}
	return nil
}

func (d *DBFileDao) CloseDBFile(ctx context.Context, dbName string) error {
	dbEntity, ok := d.databasesMap.Load(dbName)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("dbName", dbName))
		return fmt.Errorf("db not found: %s", dbName)
	}
	err := dbEntity.DBFile.Close()
	if err != nil {
		slog.ErrorContext(ctx, "close db file failed", slog.String("dbName", dbName), slog.Any("error", err))
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
