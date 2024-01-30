package test

import (
	"giangbb.studio/go.cqlx.orm/dao"
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
