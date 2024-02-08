package test

import (
	"github.com/saivnct/gocqlx-orm/dao"
	"github.com/scylladb/gocqlx/v2"
)

type CarDAO struct {
	cqlxoDAO.DAO
}

func mCarDAO(session gocqlx.Session) (*CarDAO, error) {
	d := &CarDAO{}
	err := d.InitDAO(session, Car{})
	return d, err
}
