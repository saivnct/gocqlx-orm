package test

import (
	"github.com/gocql/gocql"
)

type PriceBook struct {
	Id   gocql.UUID
	Name string
	Year int
}

func (f PriceBook) UDTName() string {
	return "price_book"
}
