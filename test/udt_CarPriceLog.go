package test

import "time"

type CarPriceLog struct {
	Price     float64   `db:"price"`
	PriceBook PriceBook `db:"price_book"`
	CreatedAt time.Time `db:"created_at"`
}

func (f CarPriceLog) UDTName() string {
	return "car_price_log"
}
