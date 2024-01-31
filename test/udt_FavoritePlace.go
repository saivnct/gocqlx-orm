package test

import (
	"giangbb.studio/go.cqlx.orm/entity"
)

type FavoritePlace struct {
	cqlxoEntity.UDT
	Place  LandMark `db:"land_mark"`
	Rating int      `db:"rating"`
}
