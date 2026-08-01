package entity

import (
	"os"

	"github.com/KitHub/kitdb/component"
)

type DatabaseEntity struct {
	Name    string
	DBFile  *os.File
	Indexes *component.SyncMap[string, *IndexEntity] // key=indexName
}

type IndexEntity struct {
	Name   string
	DBName string
}
