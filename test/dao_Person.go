package test

import (
	"github.com/saivnct/gocqlx-orm/dao"
	"github.com/scylladb/gocqlx/v3"
)

type PersonDAO struct {
	cqlxoDAO.DAO
}

func mPersonDAO(session gocqlx.Session) (*PersonDAO, error) {
	d := &PersonDAO{}
	err := d.InitDAO(session, Person{})

	return d, err
}
