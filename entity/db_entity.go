package entity

import (
	"os"

	"github.com/KitHub/kitdb/component"
	"github.com/KitHub/protocols/kitdb"
)

type DatabaseEntity struct {
	Name    string
	DBFile  *os.File
	Indices *component.SyncMap[string, *IndexEntity] // key=indexName
}

type IndexEntity struct {
	Name      string                            `json:"name"`
	DBName    string                            `json:"db_name"`
	Type      kitdb.IndexType                   `json:"type"`
	Fields    string                            `json:"fields"` // fields are separated by "-"
	Data      *component.SyncMap[string, int64] `json:"data"`   // key=key, value=position
	IndexFile *os.File
}
