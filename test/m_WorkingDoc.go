package test

import (
	"github.com/scylladb/gocqlx/v2"
	"time"
)

type WorkingDoc struct {
	gocqlx.UDT
	Name      string
	CreatedAt time.Time
}

//`CREATE TYPE IF NOT EXISTS working_document(name text, created_at timestamp)`
