package test

import "time"

type WorkingDoc struct {
	Name      string
	CreatedAt time.Time
}

//`CREATE TYPE IF NOT EXISTS working_document(name text, created_at timestamp)`
