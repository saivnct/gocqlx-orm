package test

import "github.com/scylladb/gocqlx/v2"

type FavoritePlace struct {
	gocqlx.UDT
	Place  LandMark `db:"land_mark"`
	Rating int      `db:"rating"`
}
