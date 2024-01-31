package test

import (
	"github.com/scylladb/gocqlx/v2"
	"time"
)

type CitizenIdent struct {
	gocqlx.UDT
	Id        string
	EndAt     time.Time
	CreatedAt time.Time
	Level     int
}
