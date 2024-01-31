package test

import "github.com/scylladb/gocqlx/v2"

type CarReward struct {
	gocqlx.UDT
	Name   string  `db:"name"`
	Cert   string  `db:"cert"`
	Reward float64 `db:"reward"`
}
