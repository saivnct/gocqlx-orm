package test

import (
	"github.com/gocql/gocql"
)

type Car struct {
	Id                  gocql.UUID             `db:"id" pk:"1"`
	Brand               string                 `db:"brand"`
	Model               string                 `db:"model"`
	Year                int                    `db:"year" ck:"1"`
	Colors              []string               `db:"colors"`
	PriceLogs           []CarPriceLog          `db:"price_logs"`
	Reward              CarReward              `db:"reward"` //year - reward
	Matrix              [][]int                `db:"matrix"`
	Levels              []int                  `db:"levels"`
	Distributions       map[string]int         `db:"distributions"` //country - amount
	MatrixMap           map[string][][]float64 `db:"matrix_map"`    //country - [][]
	ThisIgnoreField     string                 `db:"-"`
	thisUnexportedField string
}

func (p Car) TableName() string {
	return "car"
}
