package entity

import (
	"os"

	"github.com/KitHub/kitdb/component"
	"github.com/KitHub/protocols/kitdb"
)

type DatabaseEntity struct {
	Name    string
	DBFile  *os.File
	Indexes *component.SyncMap[string, *IndexEntity] // key=indexName
}

type IndexEntity struct {
	Name    string
	DBName  string
	Type    kitdb.IndexType
	Fields  string                            // fields are separated by "-"
	Indexes *component.SyncMap[string, int64] // key=key, value=position
}
