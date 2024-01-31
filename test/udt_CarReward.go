package test

import (
	"giangbb.studio/go.cqlx.orm/entity"
)

type CarReward struct {
	cqlxoEntity.UDT
	Name   string  `db:"name"`
	Cert   string  `db:"cert"`
	Reward float64 `db:"reward"`
}
