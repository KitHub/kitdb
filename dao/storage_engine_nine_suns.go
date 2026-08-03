package dao

import (
	"bufio"
	"context"
	"encoding/json"
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
	// todo, replace default column with the real column name
	defaultColumnName     string
	indexColumnsSeparator string
	dbFileSuffix          string
	indexFileSuffix       string
	dbFileLineKVSeparator string
	existFileOpenFlag     int
	newFileOpenFlag       int
	filePermission        os.FileMode
	lineBreak             byte
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
			defaultColumnName:     "key",
			indexColumnsSeparator: "-",
			dbFileSuffix:          "db",
			indexFileSuffix:       "idx",
			dbFileLineKVSeparator: ",",
			existFileOpenFlag:     os.O_RDWR,
			newFileOpenFlag:       os.O_RDWR | os.O_CREATE,
			filePermission:        os.FileMode(0644),
			lineBreak:             '\n',
			ninesunsStorageEngine: &NineSunsStorageEngine{},
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
			Indices: &component.SyncMap[string, *entity.IndexEntity]{},
		}
		s.databasesMap.Store(db, dbEntity)
	}
	dbFileName := db + "." + nineSunsStorageEngineVars.dbFileSuffix
	dbFile, err := createFileInFolder(ctx, s.dataDir, dbFileName, nineSunsStorageEngineVars.newFileOpenFlag, nineSunsStorageEngineVars.filePermission)
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
	_, ok = dbEntity.Indices.Load(index)
	if ok {
		slog.ErrorContext(ctx, "index already existed", slog.String("index", index))
		return fmt.Errorf("index already existed: %s", index)
	}
	indexEntity := &entity.IndexEntity{
		Name:   index,
		DBName: db,
		Type:   indexType,
		Fields: strings.Join(fields, nineSunsStorageEngineVars.indexColumnsSeparator),
		Data:   &component.SyncMap[string, int64]{},
	}
	dbEntity.Indices.Store(index, indexEntity)

	err := buildIndex(ctx, dbEntity, indexEntity)
	if err != nil {
		slog.ErrorContext(ctx, "build index failed", slog.String("db", db), slog.String("index", index), slog.Any("error", err))
		return err
	}

	// create index file for persistence
	indexFileName := dbEntity.Name + "." + index + "." + nineSunsStorageEngineVars.indexFileSuffix
	indexFile, err := createFileInFolder(ctx, s.dataDir, indexFileName, nineSunsStorageEngineVars.newFileOpenFlag, nineSunsStorageEngineVars.filePermission)
	if err != nil {
		slog.ErrorContext(ctx, "create index file failed", slog.Any("index", indexEntity), slog.String("dataDir", s.dataDir), slog.String("indexFile", indexFileName), slog.Any("error", err))
		return err
	}
	indexEntity.IndexFile = indexFile

	return nil
}

func (s *NineSunsStorageEngine) ReadKey(ctx context.Context, dbName string, key string) (string, bool, error) {
	dbEntity, ok := s.databasesMap.Load(dbName)
	if !ok {
		slog.ErrorContext(ctx, "db not found", slog.String("dbName", dbName))
		return "", false, fmt.Errorf("db not found: %s", dbName)
	}

	indexEntity, ok := determineIndex(ctx, dbEntity, []string{nineSunsStorageEngineVars.defaultColumnName})
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

	err = updateIndex(ctx, dbEntity, []string{nineSunsStorageEngineVars.defaultColumnName}, value)
	if err != nil {
		slog.ErrorContext(ctx, "update index failed", slog.String("dbName", dbName), slog.String("key", key), slog.Any("error", err))
		return err
	}

	return nil
}

func (s *NineSunsStorageEngine) Close(ctx context.Context) error {
	var err error

	nineSunsStorageEngineVars.ninesunsStorageEngine.databasesMap.Range(func(dbName string, db *entity.DatabaseEntity) bool {
		// close db file
		err = s.CloseDB(ctx, db)
		if err != nil {
			return false
		}

		// close index files
		db.Indices.Range(func(indexName string, index *entity.IndexEntity) bool {
			errForCloseIndex := s.CloseIndex(ctx, index)
			return errForCloseIndex == nil
		})
		return true
	})
	return nil
}

func (s *NineSunsStorageEngine) InitExistedDBs(ctx context.Context) error {
	dbFilePaths, err := listDBFiles(ctx, s.dataDir)
	if err != nil {
		slog.ErrorContext(ctx, "list db files failed", slog.Any("error", err))
		return err
	}

	// load db
	for _, dbFilePath := range dbFilePaths {
		dbFilePathElements := strings.Split(dbFilePath, ".")
		fileSuffix := dbFilePathElements[len(dbFilePathElements)-1]
		switch fileSuffix {
		case nineSunsStorageEngineVars.dbFileSuffix:
			databaseEntity, err := loadDBByDBFile(ctx, dbFilePath)
			if err != nil {
				slog.ErrorContext(ctx, "load db failed", slog.String("dbFilePath", dbFilePath), slog.Any("error", err))
				return err
			}
			s.databasesMap.Store(databaseEntity.Name, databaseEntity)
		default:
			slog.DebugContext(ctx, "unsupported file suffix", slog.String("fileSuffix", fileSuffix))
			continue
		}

	}

	// load index
	for _, dbFilePath := range dbFilePaths {
		dbFilePathElements := strings.Split(dbFilePath, ".")
		fileSuffix := dbFilePathElements[len(dbFilePathElements)-1]
		switch fileSuffix {
		case nineSunsStorageEngineVars.indexFileSuffix:
			indexEntity, err := loadIndexByIndexFile(ctx, dbFilePath)
			if err != nil {
				slog.ErrorContext(ctx, "load index failed", slog.String("indexFilePath", dbFilePath), slog.Any("error", err))
				return err
			}
			databaseEntity, ok := s.databasesMap.Load(indexEntity.DBName)
			if !ok {
				slog.ErrorContext(ctx, "db not found for index", slog.Any("indexEntity", indexEntity))
				return fmt.Errorf("db not found for index: %s", indexEntity.DBName)
			}
			databaseEntity.Indices.Store(indexEntity.Name, indexEntity)
		default:
			continue
		}
	}

	slog.InfoContext(ctx, "loading all dbs done")
	return nil
}

func (s *NineSunsStorageEngine) GetDBNames(ctx context.Context) ([]string, error) {
	return s.databasesMap.Keys(), nil
}

func (s *NineSunsStorageEngine) CloseDB(ctx context.Context, databaseEntity *entity.DatabaseEntity) error {
	err := databaseEntity.DBFile.Close()
	if err != nil {
		slog.ErrorContext(ctx, "close db file failed", slog.String("db", databaseEntity.Name), slog.Any("error", err))
		return err
	}
	slog.InfoContext(ctx, "close db done", slog.String("db", databaseEntity.Name))
	return nil
}

func (s *NineSunsStorageEngine) CloseIndex(ctx context.Context, indexEntity *entity.IndexEntity) error {
	// serialize index data to file
	err := serializeDatabaseIndexToDisk(ctx, indexEntity)
	if err != nil {
		return err
	}

	err = indexEntity.IndexFile.Close()
	if err != nil {
		slog.ErrorContext(ctx, "close index file failed", slog.String("index", indexEntity.Name), slog.Any("error", err))
		return err
	}
	slog.InfoContext(ctx, "close index done", slog.String("index", indexEntity.Name))
	return nil
}

// private functions ================================================================

// createFileInFolder
func createFileInFolder(ctx context.Context, folder string, filename string, openFileFlag int, openFilePerm os.FileMode) (*os.File, error) {
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

func loadDBByDBFile(ctx context.Context, dbFilePath string) (*entity.DatabaseEntity, error) {
	pathElements := strings.Split(dbFilePath, string(os.PathSeparator))
	dbFileName := pathElements[len(pathElements)-1]

	dbFileNameElements := strings.Split(dbFileName, ".")
	if len(dbFileNameElements) != 2 || dbFileNameElements[1] != nineSunsStorageEngineVars.dbFileSuffix {
		slog.ErrorContext(ctx, "loading db by file failed", slog.String("dbFilePath", dbFilePath), slog.Any("error", "invalid db file name format"))
		return nil, fmt.Errorf("invalid db file name format")
	}

	dbName := dbFileNameElements[0]

	dbFile, err := os.OpenFile(dbFilePath, nineSunsStorageEngineVars.existFileOpenFlag, nineSunsStorageEngineVars.filePermission)
	if err != nil {
		slog.ErrorContext(ctx, "open db file failed", slog.String("dbFile", dbFileName), slog.Any("error", err))
		return nil, err
	}
	databaseEntity := &entity.DatabaseEntity{
		Name:    dbName,
		DBFile:  dbFile,
		Indices: &component.SyncMap[string, *entity.IndexEntity]{},
	}
	return databaseEntity, nil
}

func loadDBByDBName(ctx context.Context, dataDir string, dbName string) (*entity.DatabaseEntity, error) {
	dbFilePath := dataDir + string(os.PathSeparator) + dbName + "." + nineSunsStorageEngineVars.dbFileSuffix
	return loadDBByDBFile(ctx, dbFilePath)
}

func loadIndexByIndexFile(ctx context.Context, indexFilePath string) (*entity.IndexEntity, error) {
	pathElements := strings.Split(indexFilePath, string(os.PathSeparator))
	indexFileName := pathElements[len(pathElements)-1]

	indexFilePathElements := strings.Split(indexFileName, ".")
	if len(indexFilePathElements) != 3 || indexFilePathElements[2] != nineSunsStorageEngineVars.indexFileSuffix {
		slog.ErrorContext(ctx, "loading index by file failed", slog.String("indexFilePath", indexFilePath), slog.Any("error", "invalid index file name format"))
		return nil, fmt.Errorf("invalid index file name format")
	}

	dbName := indexFilePathElements[0]
	indexName := indexFilePathElements[1]

	indexFile, err := os.OpenFile(indexFilePath, nineSunsStorageEngineVars.existFileOpenFlag, nineSunsStorageEngineVars.filePermission)
	if err != nil {
		slog.ErrorContext(ctx, "open index file failed", slog.String("indexFile", indexFileName), slog.Any("error", err))
		return nil, err
	}

	var indexData []byte
	buf := make([]byte, 1024)
	for {
		n, err := indexFile.Read(buf)
		if n > 0 {
			indexData = append(indexData, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			slog.ErrorContext(ctx, "read index file failed", slog.String("indexFile", indexFileName), slog.Any("error", err))
			return nil, err
		}
	}
	indexEntity := &entity.IndexEntity{}
	err = json.Unmarshal(indexData, indexEntity)
	if err != nil {
		slog.ErrorContext(ctx, "unmarshal index data failed", slog.String("indexFile", indexFileName), slog.Any("error", err))
		return nil, err
	}

	if indexEntity.Name != indexName {
		slog.ErrorContext(ctx, "read index file failed, index name mismatched", slog.String("indexFile", indexFileName), slog.Any("indexNameInData", indexEntity.Name))
		return nil, err
	}
	if indexEntity.DBName != dbName {
		slog.ErrorContext(ctx, "read index file failed, db name mismatched", slog.String("indexFile", indexFileName), slog.Any("dbNameInData", indexEntity.DBName))
		return nil, err
	}

	indexEntity.IndexFile = indexFile

	slog.InfoContext(ctx, "load index file done", slog.String("indexFilePath", indexFilePath))
	return indexEntity, nil
}

func updateIndex(ctx context.Context, dbEntity *entity.DatabaseEntity, fields []string, value string) error {
	indexEntity, ok := determineIndex(ctx, dbEntity, fields)
	if !ok {
		slog.DebugContext(ctx, "index not found, skip updating index", slog.String("dbName", dbEntity.Name), slog.Any("fields", fields))
		return nil
	}

	// todo, update index without reading the whole file, just append the new key-value pair to the end of the index file
	err := buildIndex(ctx, dbEntity, indexEntity)
	if err != nil {
		slog.ErrorContext(ctx, "build index failed", slog.String("dbName", dbEntity.Name), slog.String("indexName", indexEntity.Name), slog.Any("error", err))
		return err
	}
	return nil
}

func determineIndex(ctx context.Context, dbEntity *entity.DatabaseEntity, fields []string) (index *entity.IndexEntity, ok bool) {
	fieldsJoined := strings.Join(fields, nineSunsStorageEngineVars.indexColumnsSeparator)
	for _, tmpIndex := range dbEntity.Indices.Keys() {
		tmpIndexEntity, ok := dbEntity.Indices.Load(tmpIndex)
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

	pos, ok := indexEntity.Data.Load(key)
	if !ok {
		slog.ErrorContext(ctx, "index not found", slog.String("index", indexEntity.Name))
		return "", false, nil
	}

	_, err = dbEntity.DBFile.Seek(pos+int64(len(key))+1, io.SeekStart) // +1 is for key-value separator
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

func buildIndex(ctx context.Context, dbEntity *entity.DatabaseEntity, indexEntity *entity.IndexEntity) (err error) {
	var currentPos int64

	defer func() {
		slog.InfoContext(ctx, "build index done", slog.String("dbName", dbEntity.Name), slog.String("indexName", indexEntity.Name), slog.Any("error", err))

		// recover pos in the file
		_, seekErr := dbEntity.DBFile.Seek(currentPos, io.SeekStart)
		if seekErr != nil {
			slog.ErrorContext(ctx, "failed to recover file position", slog.String("dbName", dbEntity.Name), slog.Any("error", seekErr))
		}
	}()

	currentPos, err = dbEntity.DBFile.Seek(0, io.SeekCurrent)
	if err != nil {
		slog.ErrorContext(ctx, "seek current pos in db file failed", slog.String("dbName", dbEntity.Name), slog.Any("error", err))
		return err
	}

	_, err = dbEntity.DBFile.Seek(0, io.SeekStart)
	if err != nil {
		slog.ErrorContext(ctx, "seek start pos in db file failed", slog.String("dbName", dbEntity.Name), slog.Any("error", err))
		return err
	}

	scanner := bufio.NewScanner(dbEntity.DBFile)
	lineNum := 0
	bytesRead := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		kv := strings.SplitN(line, nineSunsStorageEngineVars.dbFileLineKVSeparator, 2)
		indexEntity.Data.Store(kv[0], int64(bytesRead))
		bytesRead += len(line) + 1 // +1 for line break
	}
	if err := scanner.Err(); err != nil {
		slog.ErrorContext(ctx, "scan db file failed", slog.String("dbName", dbEntity.Name), slog.Any("error", err))
		return err
	}
	return nil
}

func serializeDatabaseIndexToDisk(ctx context.Context, indexEntity *entity.IndexEntity) error {
	// todo, upgrade serialization in case of big index data
	bytes, err := json.Marshal(indexEntity)
	if err != nil {
		slog.ErrorContext(ctx, "serialize index failed", slog.Any("index", indexEntity), slog.Any("error", err))
		return err
	}

	// overwrite the whole file
	_, err = indexEntity.IndexFile.Seek(0, io.SeekStart)
	if err != nil {
		slog.ErrorContext(ctx, "seek index file pos to start failed", slog.Any("indexEntity", indexEntity), slog.Any("error", err))
		return err
	}

	n, err := indexEntity.IndexFile.Write(bytes)
	if err != nil {
		slog.ErrorContext(ctx, "write index data to file failed", slog.Any("index", indexEntity), slog.Any("error", err))
		return err
	}
	slog.ErrorContext(ctx, "serialize index to file done", slog.Any("index", indexEntity), slog.Any("bytesCount", n))
	return nil
}
