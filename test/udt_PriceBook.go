package test

import (
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
)

type PriceBook struct {
	gocqlx.UDT
	Id   gocql.UUID
	Name string
	Year int
}
