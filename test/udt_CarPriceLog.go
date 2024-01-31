package test

import (
	"github.com/scylladb/gocqlx/v2"
	"time"
)

type CarPriceLog struct {
	gocqlx.UDT
	Price     float64   `db:"price"`
	PriceBook PriceBook `db:"price_book"`
	CreatedAt time.Time `db:"created_at"`
}
