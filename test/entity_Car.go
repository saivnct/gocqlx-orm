package test

import (
	"github.com/gocql/gocql"
)

type Car struct {
	Id                  gocql.UUID             `db:"id" pk:"1"`
	Brand               string                 `db:"brand" index:"true"`
	Model               string                 `db:"model" index:"true"`
	Year                int                    `db:"year" ck:"1"`
	Name                string                 `db:"name" `
	Colors              []string               `db:"colors"`
	PriceLog            CarPriceLog            `db:"price_log"`
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
