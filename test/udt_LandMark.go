package test

import (
	"giangbb.studio/go.cqlx.orm/entity"
)

type LandMark struct {
	cqlxoEntity.UDT
	City       string   `db:"city"`
	Country    string   `db:"country"`
	Population int64    `db:"population"`
	CheckPoint []string `db:"check_point"`
}
