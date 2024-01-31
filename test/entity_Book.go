package test

import (
	"github.com/gocql/gocql"
)

type Book struct {
	Id              gocql.UUID `db:"id" dbType:"timeuuid"`
	Name            string     `db:"name" pk:"2"`
	Author          string     `pk:"1"` // not declare db:"author" -> default db:"author"
	Content         string
	WorkingDocument WorkingDoc `dbType:"working_document"`
}

func (p Book) TableName() string {
	return "book"
}
