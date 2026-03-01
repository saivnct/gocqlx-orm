package test

import "github.com/scylladb/gocqlx/v3"

type Category struct {
	gocqlx.UDT
	Name string `db:"name"`
	Code int    `db:"code"`
}
