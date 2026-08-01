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

type nineSunsStorageEngineVarsStruct struct {
	dbFileSuffix          string
	dbIndexFileSuffix     string
	dbFileLineKVSeparator string
	dbExistFileOpenFlag   int
	dbCreateFileOpenFlag  int
	dbFilePermission      os.FileMode
	lineBreak             string
	ninesunsStorageEngine *NineSunsStorageEngine
}

var onceForNineSunsStorageEngine sync.Once = sync.Once{}
var nineSunsStorageEngineVars *nineSunsStorageEngineVarsStruct

// NineSunsStorageEngine, storing data in files, by appending data to the end of the file
type NineSunsStorageEngine struct {
	dataDir               string
	databasesMap          *component.SyncMap[string, *entity.DatabaseEntity] // key=dbname, and also dbFileName(without ".db")
	initCallbackComponent *component.InitComponent
	shutdownComponent     *component.ShutdownComponent
}

func NewNineSunsStorageEngine(ctx context.Context, dataDir string, initCallbackComponent *component.InitComponent, shutdownCallbackComponent *component.ShutdownComponent) StorageEngine {
	onceForNineSunsStorageEngine.Do(func() {
		nineSunsStorageEngineVars = &nineSunsStorageEngineVarsStruct{
			dbFileSuffix:          "db",
			dbIndexFileSuffix:     "idx",
			dbFileLineKVSeparator: ",",
			dbExistFileOpenFlag:   os.O_RDWR | os.O_APPEND,
			dbCreateFileOpenFlag:  os.O_RDWR | os.O_APPEND | os.O_CREATE,
			dbFilePermission:      os.FileMode(0644),
			lineBreak:             "",
			ninesunsStorageEngine: &NineSunsStorageEngine{},
		}

		if runtime.GOOS == "windows" {
			nineSunsStorageEngineVars.lineBreak = "\r\n"
		} else {
			nineSunsStorageEngineVars.lineBreak = "\n"
		}

		nineSunsStorageEngineVars.ninesunsStorageEngine = &NineSunsStorageEngine{
			dataDir:               dataDir,
			databasesMap:          &component.SyncMap[string, *entity.DatabaseEntity]{},
			initCallbackComponent: initCallbackComponent,
			shutdownComponent:     shutdownCallbackComponent,
		}

		nineSunsStorageEngineVars.ninesunsStorageEngine.initCallbackComponent.RegisterInitCallback(func(ctx context.Context) error {
			return nineSunsStorageEngineVars.ninesunsStorageEngine.InitExistedDBs(ctx)
		})

		nineSunsStorageEngineVars.ninesunsStorageEngine.shutdownComponent.RegisterShutdownCallback(func(ctx context.Context) error {
			return nineSunsStorageEngineVars.ninesunsStorageEngine.Close(ctx)
		})
	})
	return nineSunsStorageEngineVars.ninesunsStorageEngine
}

func (s *NineSunsStorageEngine) CreateDB(ctx context.Context, db string) error {
	dbEntity, ok := s.databasesMap.Load(db)
	if !ok {
		dbEntity = &entity.DatabaseEntity{
			Name:    db,
			DBFile:  nil,
			Indexes: &component.SyncMap[string, *entity.IndexEntity]{},
		}
		s.databasesMap.Store(db, dbEntity)
	}
	dbFileName := db + "." + nineSunsStorageEngineVars.dbFileSuffix
	dbFile, err := createFileInFolder(s.dataDir, dbFileName, nineSunsStorageEngineVars.dbCreateFileOpenFlag, nineSunsStorageEngineVars.dbFilePermission)
	if err != nil {
		slog.ErrorContext(ctx, "create db file failed", slog.String("dataDir", s.dataDir), slog.String("dbFileName", dbFileName), slog.Any("error", err))
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

func (s *NineSunsStorageEngine) ReadKey(ctx context.Context, dbName string, key string) (string, error) {
	dbEntity, ok := s.databasesMap.Load(dbName)
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
		kv := strings.SplitN(line, nineSunsStorageEngineVars.dbFileLineKVSeparator, 2)
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

func (s *NineSunsStorageEngine) WriteKeyValue(ctx context.Context, dbName string, key string, value string) error {
	dbEntity, ok := s.databasesMap.Load(dbName)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("dbName", dbName))
		return fmt.Errorf("db not found: %s", dbName)
	}
	_, err := dbEntity.DBFile.WriteString(key + nineSunsStorageEngineVars.dbFileLineKVSeparator + value + nineSunsStorageEngineVars.lineBreak)
	if err != nil {
		slog.ErrorContext(ctx, "append db file failed", slog.String("dbName", dbName))
		return err
	}
	return nil
}

func (s *NineSunsStorageEngine) Close(ctx context.Context) error {
	dbNames := nineSunsStorageEngineVars.ninesunsStorageEngine.databasesMap.Keys()
	for _, dbName := range dbNames {
		_ = nineSunsStorageEngineVars.ninesunsStorageEngine.CloseDBFile(ctx, dbName)
	}
	return nil
}

func (s *NineSunsStorageEngine) InitExistedDBs(ctx context.Context) error {
	dbFiles, err := listDBFiles(ctx, s.dataDir)
	if err != nil {
		slog.ErrorContext(ctx, "list db files failed", slog.Any("error", err))
		return err
	}

	for _, dbFile := range dbFiles {
		databaseEntity, err := loadDBByDBFile(ctx, dbFile)
		if err != nil {
			slog.ErrorContext(ctx, "load db failed", slog.String("dbFile", dbFile), slog.Any("error", err))
			return err
		}
		s.databasesMap.Store(databaseEntity.Name, databaseEntity)
	}

	slog.InfoContext(ctx, "loading all dbs done")
	return nil
}

func (s *NineSunsStorageEngine) GetDBNames(ctx context.Context) ([]string, error) {
	return s.databasesMap.Keys(), nil
}

func (s *NineSunsStorageEngine) CloseDBFile(ctx context.Context, dbName string) error {
	dbEntity, ok := s.databasesMap.Load(dbName)
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
func createFileInFolder(folder string, filename string, openFileFlag int, openFilePerm os.FileMode) (*os.File, error) {
	fullPath := filepath.Join(folder, filename)
	return os.OpenFile(fullPath, openFileFlag, openFilePerm)
}

func initDBFile(ctx context.Context, dbEntity *entity.DatabaseEntity) error {
	slog.InfoContext(ctx, "init db file done", slog.String("db", dbEntity.Name), slog.String("dbFile", dbEntity.DBFile.Name()))
	return nil
}

func listDBFiles(ctx context.Context, dataDir string) ([]string, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		slog.ErrorContext(ctx, "read data dir failed", slog.String("dataDir", dataDir), slog.Any("error", err))
		return nil, err
	}

	retval := []string{}
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			continue
		}
		retval = append(retval, dataDir+string(os.PathSeparator)+name)
	}
	return retval, nil
}

func loadDBByDBFile(ctx context.Context, dataDir string) (*entity.DatabaseEntity, error) {
	dbFilePathElements := strings.Split(dataDir, ".")
	if dbFilePathElements[len(dbFilePathElements)-1] != nineSunsStorageEngineVars.dbFileSuffix {
		slog.ErrorContext(ctx, "loading db by file failed", slog.String("dbFilePath", dataDir), slog.Any("error", "invalid db file name format"))
		return nil, fmt.Errorf("invalid db file name format")
	}

	pathElements := strings.Split(dataDir, string(os.PathSeparator))
	dbFileName := pathElements[len(pathElements)-1]
	dbFileElements := strings.Split(dbFileName, ".")
	dbName := dbFileElements[0]

	dbFile, err := os.OpenFile(dataDir, nineSunsStorageEngineVars.dbExistFileOpenFlag, nineSunsStorageEngineVars.dbFilePermission)
	if err != nil {
		slog.ErrorContext(ctx, "open db file failed", slog.String("dbFile", dbFileName), slog.Any("error", err))
		return nil, err
	}
	databaseEntity := &entity.DatabaseEntity{
		Name:   dbName,
		DBFile: dbFile,
		// todo, load indexes from index file
		Indexes: &component.SyncMap[string, *entity.IndexEntity]{},
	}
	return databaseEntity, nil
}

func loadDBByDBName(ctx context.Context, dataDir string, dbName string) (*entity.DatabaseEntity, error) {
	dbFilePath := dataDir + string(os.PathSeparator) + dbName + "." + nineSunsStorageEngineVars.dbFileSuffix
	return loadDBByDBFile(ctx, dbFilePath)
}
