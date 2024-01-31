package test

import (
	"github.com/scylladb/gocqlx/v2"
	"time"
)

type CarPriceLog struct {
	gocqlx.UDT
	Price     float64   `db:"price"`
	CreatedAt time.Time `db:"created_at"`
}
