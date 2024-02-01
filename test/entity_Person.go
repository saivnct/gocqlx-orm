package test

import (
	"github.com/gocql/gocql"
	"time"
)

type Person struct {
	Id             gocql.UUID    `db:"id" dbType:"timeuuid"`
	LastName       string        `db:"last_name" pk:"2" index:"true"`
	FirstName      string        `pk:"1" index:"true"` // not declare db:"first_name" -> default db:"first_name"
	FavoritePlace  FavoritePlace `db:"favorite_place"`
	Email          string        `index:"true"` //not declare db:"email" -> default db:"email"
	StaticIP       string        `db:"static_ip" dbType:"inet"`
	Nicknames      []string      `db:"nick_names" dbType:"set<text>"`
	WorkingHistory map[int]string
	CitizenIdent   CitizenIdent `db:"citizen_ident" dbType:"citizen_id"`
	CreatedAt      time.Time    `db:"created_at" ck:"1"`
}

func (p Person) TableName() string {
	return "person"
}
