package dao

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/KitHub/kitdb/component"
	"github.com/KitHub/kitdb/entity"
)

const (
	dbFileSuffix                      = "db"
	dbFileLineKVSeparator             = ","
	dbExistFileOpenFlag               = os.O_RDWR | os.O_APPEND
	dbCreateFileOpenFlag              = os.O_RDWR | os.O_APPEND | os.O_CREATE
	dbFilePermission      os.FileMode = 0644
)

var lineBreak string
var dbFileDao *DBFileDao
var onceForDBFileDao sync.Once

type DBFileDao struct {
	dataDir           string
	databasesMap      *component.SyncMap[string, *entity.DatabaseEntity] // key=dbname, and also dbFileName(without ".db")
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
	dbFile, err := createFileInFolder(d.dataDir, dbFileName, dbCreateFileOpenFlag, dbFilePermission)
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

func (d *DBFileDao) ReadKey(ctx context.Context, dbName string, key string) (string, error) {
	dbEntity, ok := d.databasesMap.Load(dbName)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("dbName", dbName))
		return "", fmt.Errorf("db not found: %s", dbName)
	}
	scanner := bufio.NewScanner(dbEntity.DBFile)
	lineNum := 0
	var lastV string
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		kv := strings.SplitN(line, dbFileLineKVSeparator, 2)
		if kv[0] == key {
			lastV = kv[1]
		}
	}
	if err := scanner.Err(); err != nil {
		slog.ErrorContext(ctx, "scan db file failed", slog.String("dbName", dbName), slog.Any("error", err))
		return "", err
	}
	return lastV, nil
}

func (d *DBFileDao) WriteKeyValue(ctx context.Context, dbName string, key string, value string) error {
	dbEntity, ok := d.databasesMap.Load(dbName)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("dbName", dbName))
		return fmt.Errorf("db not found: %s", dbName)
	}
	_, err := dbEntity.DBFile.WriteString(key + dbFileLineKVSeparator + value + lineBreak)
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

func (d *DBFileDao) ListDBFiles(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(d.dataDir)
	if err != nil {
		slog.ErrorContext(ctx, "read data dir failed", slog.String("dataDir", d.dataDir), slog.Any("error", err))
		return nil, err
	}

	retval := []string{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			continue
		}
		retval = append(retval, d.dataDir+string(os.PathSeparator)+name)
	}
	return retval, nil
}

func (d *DBFileDao) LoadDBByDBFile(ctx context.Context, dbFilePath string) (*entity.DatabaseEntity, error) {
	dbFilePathElements := strings.Split(dbFilePath, ".")
	if dbFilePathElements[len(dbFilePathElements)-1] != dbFileSuffix {
		slog.ErrorContext(ctx, "loading db by file failed", slog.String("dbFilePath", dbFilePath), slog.Any("error", "invalid db file name format"))
		return nil, fmt.Errorf("invalid db file name format")
	}

	pathElements := strings.Split(dbFilePath, string(os.PathSeparator))
	dbFileName := pathElements[len(pathElements)-1]
	dbFileElements := strings.Split(dbFileName, ".")
	dbName := dbFileElements[0]

	dbFile, err := os.OpenFile(dbFilePath, dbExistFileOpenFlag, dbFilePermission)
	if err != nil {
		slog.ErrorContext(ctx, "open db file failed", slog.String("dbFile", dbFileName), slog.Any("error", err))
		return nil, err
	}
	databaseEntity := &entity.DatabaseEntity{
		Name:   dbName,
		DBFile: dbFile,
	}
	d.databasesMap.Store(dbName, databaseEntity)

	return databaseEntity, nil
}

func (d *DBFileDao) LoadDBByDBName(ctx context.Context, dbName string) (*entity.DatabaseEntity, error) {
	dbFilePath := d.dataDir + string(os.PathSeparator) + dbName + "." + dbFileSuffix
	return d.LoadDBByDBFile(ctx, dbFilePath)
}

// private functions ================================================================

// createFileInFolder
func createFileInFolder(folder string, filename string, openFileFlag int, openFilePerm os.FileMode) (*os.File, error) {
	fullPath := filepath.Join(folder, filename)
	return os.OpenFile(fullPath, openFileFlag, openFilePerm)
}

func initDBFile(ctx context.Context, dbEntity *entity.DatabaseEntity) error {
	slog.InfoContext(ctx, "init db file done", slog.String("db", dbEntity.Name), slog.String("dbFile", dbEntity.DBFile.Name()))
	return nil
}
