package test

import (
	cqlxoDAO "giangbb.studio/go.cqlx.orm/dao"
	"github.com/scylladb/gocqlx/v2"
)

type PersonDAO struct {
	cqlxoDAO.DAO
}

func mPersonDAO(session gocqlx.Session) (*PersonDAO, error) {
	d := &PersonDAO{}
	err := d.InitDAO(session, Person{})

	return d, err
}
