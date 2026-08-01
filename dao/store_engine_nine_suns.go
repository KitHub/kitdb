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

type nineSunsStoreEngineVarsStruct struct {
	dbFileSuffix          string
	dbFileLineKVSeparator string
	dbExistFileOpenFlag   int
	dbCreateFileOpenFlag  int
	dbFilePermission      os.FileMode
	lineBreak             string
	ninesunsStoreEngine   *NineSunsStoreEngine
}

var onceForNineSunsStoreEngine sync.Once = sync.Once{}
var nineSunsStoreEngineVars *nineSunsStoreEngineVarsStruct

type NineSunsStoreEngine struct {
	dataDir               string
	databasesMap          *component.SyncMap[string, *entity.DatabaseEntity] // key=dbname, and also dbFileName(without ".db")
	initCallbackComponent *component.InitComponent
	shutdownComponent     *component.ShutdownComponent
}

func NewNineSunsStoreEngine(ctx context.Context, dataDir string, initCallbackComponent *component.InitComponent, shutdownCallbackComponent *component.ShutdownComponent) StoreEngine {
	onceForNineSunsStoreEngine.Do(func() {
		nineSunsStoreEngineVars = &nineSunsStoreEngineVarsStruct{
			dbFileSuffix:          "db",
			dbFileLineKVSeparator: ",",
			dbExistFileOpenFlag:   os.O_RDWR | os.O_APPEND,
			dbCreateFileOpenFlag:  os.O_RDWR | os.O_APPEND | os.O_CREATE,
			dbFilePermission:      os.FileMode(0644),
			lineBreak:             "",
			ninesunsStoreEngine:   &NineSunsStoreEngine{},
		}

		if runtime.GOOS == "windows" {
			nineSunsStoreEngineVars.lineBreak = "\r\n"
		} else {
			nineSunsStoreEngineVars.lineBreak = "\n"
		}

		nineSunsStoreEngineVars.ninesunsStoreEngine = &NineSunsStoreEngine{
			dataDir:               dataDir,
			databasesMap:          &component.SyncMap[string, *entity.DatabaseEntity]{},
			initCallbackComponent: initCallbackComponent,
			shutdownComponent:     shutdownCallbackComponent,
		}

		nineSunsStoreEngineVars.ninesunsStoreEngine.initCallbackComponent.RegisterInitCallback(func(ctx context.Context) error {
			return nineSunsStoreEngineVars.ninesunsStoreEngine.InitExistedDBs(ctx)
		})

		nineSunsStoreEngineVars.ninesunsStoreEngine.shutdownComponent.RegisterShutdownCallback(func(ctx context.Context) error {
			return nineSunsStoreEngineVars.ninesunsStoreEngine.Close(ctx)
		})
	})
	return nineSunsStoreEngineVars.ninesunsStoreEngine
}

func (s *NineSunsStoreEngine) CreateDB(ctx context.Context, db string) error {
	dbEntity, ok := s.databasesMap.Load(db)
	if !ok {
		dbEntity = &entity.DatabaseEntity{
			Name:   db,
			DBFile: nil,
		}
		s.databasesMap.Store(db, dbEntity)
	}
	dbFileName := db + "." + nineSunsStoreEngineVars.dbFileSuffix
	dbFile, err := createFileInFolder(s.dataDir, dbFileName, nineSunsStoreEngineVars.dbCreateFileOpenFlag, nineSunsStoreEngineVars.dbFilePermission)
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

func (s *NineSunsStoreEngine) ReadKey(ctx context.Context, dbName string, key string) (string, error) {
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
		kv := strings.SplitN(line, nineSunsStoreEngineVars.dbFileLineKVSeparator, 2)
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

func (s *NineSunsStoreEngine) WriteKeyValue(ctx context.Context, dbName string, key string, value string) error {
	dbEntity, ok := s.databasesMap.Load(dbName)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("dbName", dbName))
		return fmt.Errorf("db not found: %s", dbName)
	}
	_, err := dbEntity.DBFile.WriteString(key + nineSunsStoreEngineVars.dbFileLineKVSeparator + value + nineSunsStoreEngineVars.lineBreak)
	if err != nil {
		slog.ErrorContext(ctx, "append db file failed", slog.String("dbName", dbName))
		return err
	}
	return nil
}

func (s *NineSunsStoreEngine) Close(ctx context.Context) error {
	dbNames := nineSunsStoreEngineVars.ninesunsStoreEngine.databasesMap.Keys()
	for _, dbName := range dbNames {
		_ = nineSunsStoreEngineVars.ninesunsStoreEngine.CloseDBFile(ctx, dbName)
	}
	return nil
}

func (s *NineSunsStoreEngine) InitExistedDBs(ctx context.Context) error {
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

func (s *NineSunsStoreEngine) GetDBNames(ctx context.Context) ([]string, error) {
	return s.databasesMap.Keys(), nil
}

func (s *NineSunsStoreEngine) CloseDBFile(ctx context.Context, dbName string) error {
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
	if dbFilePathElements[len(dbFilePathElements)-1] != nineSunsStoreEngineVars.dbFileSuffix {
		slog.ErrorContext(ctx, "loading db by file failed", slog.String("dbFilePath", dataDir), slog.Any("error", "invalid db file name format"))
		return nil, fmt.Errorf("invalid db file name format")
	}

	pathElements := strings.Split(dataDir, string(os.PathSeparator))
	dbFileName := pathElements[len(pathElements)-1]
	dbFileElements := strings.Split(dbFileName, ".")
	dbName := dbFileElements[0]

	dbFile, err := os.OpenFile(dataDir, nineSunsStoreEngineVars.dbExistFileOpenFlag, nineSunsStoreEngineVars.dbFilePermission)
	if err != nil {
		slog.ErrorContext(ctx, "open db file failed", slog.String("dbFile", dbFileName), slog.Any("error", err))
		return nil, err
	}
	databaseEntity := &entity.DatabaseEntity{
		Name:   dbName,
		DBFile: dbFile,
	}
	return databaseEntity, nil
}

func loadDBByDBName(ctx context.Context, dataDir string, dbName string) (*entity.DatabaseEntity, error) {
	dbFilePath := dataDir + string(os.PathSeparator) + dbName + "." + nineSunsStoreEngineVars.dbFileSuffix
	return loadDBByDBFile(ctx, dbFilePath)
}
