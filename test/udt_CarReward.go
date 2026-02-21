package test

import "github.com/scylladb/gocqlx/v3"

type CarReward struct {
	gocqlx.UDT
	Name   string  `db:"name"`
	Cert   string  `db:"cert"`
	Reward float64 `db:"reward"`
}
