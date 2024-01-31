package test

import (
	"giangbb.studio/go.cqlx.orm/entity"
	"time"
)

type CarPriceLog struct {
	cqlxoEntity.UDT
	Price     float64   `db:"price"`
	PriceBook PriceBook `db:"price_book"`
	CreatedAt time.Time `db:"created_at"`
}
