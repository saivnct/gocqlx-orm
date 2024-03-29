package test

import "github.com/scylladb/gocqlx/v2"

type FavoritePlace struct {
	gocqlx.UDT
	City       string   `db:"city"`
	Country    string   `db:"country"`
	Population int64    `db:"population"`
	CheckPoint []string `db:"check_point"`
	Rating     int      `db:"rating"`
}
