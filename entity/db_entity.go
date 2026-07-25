package entity

import "os"

type DatabaseEntity struct {
	Name   string
	DBFile *os.File
}
