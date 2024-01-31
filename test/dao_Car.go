package test

import (
	"giangbb.studio/go.cqlx.orm/dao"
	"github.com/gocql/gocql"
)

type CarDAO struct {
	cqlxoDAO.DAO
}

func mCarDAO(session *gocql.Session) (*CarDAO, error) {
	d := &CarDAO{}
	err := d.InitDAO(session, Car{})
	return d, err
}
