package test

import (
	"github.com/gocql/gocql"
	"time"
)

type Tweet struct {
	TimeLine  string
	Id        gocql.UUID `pk:"1"`
	Text      string
	CreatedAt time.Time ` ck:"1"`
}

func (p Tweet) TableName() string {
	return "tweet"
}
