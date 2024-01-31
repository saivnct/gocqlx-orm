package test

import (
	cqlxoDAO "giangbb.studio/go.cqlx.orm/dao"
	"github.com/gocql/gocql"
)

type BookDAO struct {
	cqlxoDAO.DAO
}

func mBookDAO(session *gocql.Session) (*BookDAO, error) {
	d := &BookDAO{}
	err := d.InitDAO(session, Book{})

	return d, err
}
