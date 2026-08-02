package dao

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/KitHub/kitdb/component"
	"github.com/KitHub/kitdb/entity"
	"github.com/KitHub/protocols/kitdb"
)

type nineSunsStorageEngineVarsStruct struct {
	dbIndexFieldsSeparator string
	dbFileSuffix           string
	dbIndexFileSuffix      string
	dbFileLineKVSeparator  string
	dbExistFileOpenFlag    int
	dbCreateFileOpenFlag   int
	dbFilePermission       os.FileMode
	lineBreak              byte
	ninesunsStorageEngine  *NineSunsStorageEngine
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
			dbIndexFieldsSeparator: "-",
			dbFileSuffix:           "db",
			dbIndexFileSuffix:      "idx",
			dbFileLineKVSeparator:  ",",
			dbExistFileOpenFlag:    os.O_RDWR,
			dbCreateFileOpenFlag:   os.O_RDWR | os.O_CREATE,
			dbFilePermission:       os.FileMode(0644),
			lineBreak:              '\n',
			ninesunsStorageEngine:  &NineSunsStorageEngine{},
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

func (s *NineSunsStorageEngine) CreateIndex(ctx context.Context, db string, index string, indexType kitdb.IndexType, fields []string) error {
	dbEntity, ok := s.databasesMap.Load(db)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("db", db))
		return fmt.Errorf("db not found: %s", db)
	}
	_, ok = dbEntity.Indexes.Load(index)
	if ok {
		slog.ErrorContext(ctx, "index already existed", slog.String("index", index))
		return fmt.Errorf("index already existed: %s", index)
	}
	indexEntity := &entity.IndexEntity{
		Name:    index,
		DBName:  db,
		Type:    indexType,
		Fields:  strings.Join(fields, nineSunsStorageEngineVars.dbIndexFieldsSeparator),
		Indexes: &component.SyncMap[string, int64]{},
	}
	dbEntity.Indexes.Store(index, indexEntity)
	return nil
}

func (s *NineSunsStorageEngine) ReadKey(ctx context.Context, dbName string, key string) (string, bool, error) {
	dbEntity, ok := s.databasesMap.Load(dbName)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("dbName", dbName))
		return "", false, fmt.Errorf("db not found: %s", dbName)
	}

	indexEntity, ok := determineIndex(ctx, dbEntity, []string{key})
	if ok {
		v, ok, err := queryKeyByIndex(ctx, dbEntity, indexEntity, key)
		if err != nil {
			slog.ErrorContext(ctx, "query key by index failed", slog.String("dbName", dbName), slog.String("key", key), slog.Any("error", err))
			return "", false, err
		}
		return v, ok, nil
	}

	v, ok, err := queryKeyByReadingFile(ctx, dbEntity, key)
	if err != nil {
		slog.ErrorContext(ctx, "query key by reading file failed", slog.String("dbName", dbName), slog.String("key", key), slog.Any("error", err))
		return "", false, err
	}

	return v, ok, nil
}

func (s *NineSunsStorageEngine) WriteKeyValue(ctx context.Context, dbName string, key string, value string) error {
	dbEntity, ok := s.databasesMap.Load(dbName)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("dbName", dbName))
		return fmt.Errorf("db not found: %s", dbName)
	}

	_, err := dbEntity.DBFile.Seek(0, io.SeekEnd) // Move the file pointer to the end of the file before writing
	if err != nil {
		slog.ErrorContext(ctx, "seek db file failed", slog.String("dbName", dbName), slog.Any("error", err))
		return err
	}

	_, err = dbEntity.DBFile.WriteString(key + nineSunsStorageEngineVars.dbFileLineKVSeparator + value + string(nineSunsStorageEngineVars.lineBreak))
	if err != nil {
		slog.ErrorContext(ctx, "append db file failed", slog.String("dbName", dbName), slog.Any("error", err))
		return err
	}

	err = updateIndex(ctx, dbEntity, []string{key}, value)
	if err != nil {
		slog.ErrorContext(ctx, "update index failed", slog.String("dbName", dbName), slog.String("key", key), slog.Any("error", err))
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
	// todo, load db and index
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
		Name:    dbName,
		DBFile:  dbFile,
		Indexes: &component.SyncMap[string, *entity.IndexEntity]{},
	}
	return databaseEntity, nil
}

func loadDBByDBName(ctx context.Context, dataDir string, dbName string) (*entity.DatabaseEntity, error) {
	dbFilePath := dataDir + string(os.PathSeparator) + dbName + "." + nineSunsStorageEngineVars.dbFileSuffix
	return loadDBByDBFile(ctx, dbFilePath)
}

func updateIndex(ctx context.Context, dbEntity *entity.DatabaseEntity, fields []string, value string) error {
	_, ok := determineIndex(ctx, dbEntity, fields)
	if !ok {
		slog.DebugContext(ctx, "index not found, skip updating index", slog.String("dbName", dbEntity.Name), slog.Any("fields", fields))
		return nil
	}

	// todo, update index by reading db file and updating index file
	return nil
}

func determineIndex(ctx context.Context, dbEntity *entity.DatabaseEntity, fields []string) (index *entity.IndexEntity, ok bool) {
	fieldsJoined := strings.Join(fields, nineSunsStorageEngineVars.dbIndexFieldsSeparator)
	for _, tmpIndex := range dbEntity.Indexes.Keys() {
		tmpIndexEntity, ok := dbEntity.Indexes.Load(tmpIndex)
		if !ok {
			slog.ErrorContext(ctx, "index not found", slog.String("index", tmpIndex))
			return nil, false
		}
		if tmpIndexEntity.Fields == fieldsJoined {
			// todo, find all matched indexes, then return the best one
			return tmpIndexEntity, true
		}

	}
	return nil, false
}

func queryKeyByIndex(ctx context.Context, dbEntity *entity.DatabaseEntity, indexEntity *entity.IndexEntity, key string) (value string, ok bool, err error) {
	currentFilePos, err := dbEntity.DBFile.Seek(0, io.SeekCurrent)
	if err != nil {
		slog.ErrorContext(ctx, "seek current pos in db file failed", slog.String("dbName", dbEntity.Name), slog.Any("error", err))
		return "", false, err
	}

	pos, ok := indexEntity.Indexes.Load(key)
	if !ok {
		slog.ErrorContext(ctx, "index not found", slog.String("index", indexEntity.Name))
		return "", false, fmt.Errorf("key not found in index: %s", key)
	}

	_, err = dbEntity.DBFile.Seek(pos+1, io.SeekStart) // +1 is for key-value separator
	if err != nil {
		slog.ErrorContext(ctx, "seek index pos in db file failed", slog.String("dbName", dbEntity.Name), slog.Any("error", err))
		return "", false, err
	}

	buf := make([]byte, 1024)
	n, err := dbEntity.DBFile.Read(buf)
	if err != nil {
		slog.ErrorContext(ctx, "read db file failed", slog.String("dbName", dbEntity.Name), slog.Any("error", err))
		return "", false, err
	}

	for i := 0; i < n; i++ {
		if buf[i] == nineSunsStorageEngineVars.lineBreak {
			value = string(buf[:i])
			break
		}
	}

	// restore file pos
	_, err = dbEntity.DBFile.Seek(currentFilePos, io.SeekStart)
	if err != nil {
		slog.ErrorContext(ctx, "seek origin pos indb file failed", slog.String("dbName", dbEntity.Name), slog.Any("error", err))
		return "", false, err
	}
	return value, true, nil
}

func queryKeyByReadingFile(ctx context.Context, dbEntity *entity.DatabaseEntity, key string) (value string, ok bool, err error) {
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
		slog.ErrorContext(ctx, "scan db file failed", slog.String("dbName", dbEntity.Name), slog.Any("error", err))
		return "", false, err
	}
	return lastV, true, nil
}
