package test

import "github.com/scylladb/gocqlx/v3"

type Address struct {
	gocqlx.UDT
	Street string `db:"street"`
	City   string `db:"city"`
	Zip    int    `db:"zip"`
}
