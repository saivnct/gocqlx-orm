package test

import (
	"github.com/gocql/gocql"
)

type Book struct {
	Id      gocql.UUID `dbType:"timeuuid"`
	Name    string     `pk:"2"` // not declare db:"name" -> default db:"name"
	Author  string     `pk:"1"` // not declare db:"author" -> default db:"author"
	Content string
}

func (p Book) TableName() string {
	return "book"
}
