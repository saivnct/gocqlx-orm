package test

import (
	"github.com/gocql/gocql"
)

type Delivery struct {
	Id      gocql.UUID      `db:"id" pk:"1"`
	Name    string          `db:"name" index:"true"`
	Profile DeliveryProfile `db:"profile"`
	DropAt  Coordinate      `db:"drop_at"`
}

func (Delivery) TableName() string {
	return "delivery"
}
