package test

import (
	cqlxoDAO "giangbb.studio/go.cqlx.orm/dao"
	"github.com/gocql/gocql"
)

type PersonDAO struct {
	cqlxoDAO.DAO
}

func mPersonDAO(session *gocql.Session) (*PersonDAO, error) {
	d := &PersonDAO{}
	err := d.InitDAO(session, Person{})

	return d, err
}
