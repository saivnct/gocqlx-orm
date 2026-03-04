package test

import "github.com/gocql/gocql"

type BinaryDocument struct {
	ID       gocql.UUID `db:"id" pk:"1"`
	Name     string     `db:"name" index:"true"`
	Payload  []byte     `db:"payload"`
	Checksum []byte     `db:"checksum" dbType:"blob"`
}

func (BinaryDocument) TableName() string {
	return "binary_document"
}
