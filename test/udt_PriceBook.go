package test

import (
	"giangbb.studio/go.cqlx.orm/entity"
	"github.com/gocql/gocql"
)

type PriceBook struct {
	cqlxoEntity.UDT
	Id   gocql.UUID
	Name string
	Year int
}
