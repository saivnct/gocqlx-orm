package test

import (
	"github.com/gocql/gocql"
	"time"
)

type Person struct {
	Id               gocql.UUID     `db:"id" dbType:"timeuuid"`
	LastName         string         `db:"last_name" pk:"2"`
	FirstName        string         `pk:"1"` // not declare db:"first_name" -> default db:"first_name"
	FavoritePlace    FavoritePlace  `db:"favorite_place"`
	Email            string         //not declare db:"email" -> default db:"email"
	StaticIP         string         `db:"static_ip" dbType:"inet"`
	Nicknames        []string       `db:"nick_names" dbType:"set<text>"`
	WorkingHistory   map[int]string `dbType:"map<int, text>"`
	WorkingDocuments []WorkingDoc   `dbType:"list<frozen<working_document>>"`
	CitizenIdent     CitizenIdent   `dbType:"tuple<text, timestamp, timestamp, int>"`
	CreatedAt        time.Time      `db:"created_at" ck:"1"`
}

func (p Person) TableName() string {
	return "person"
}
