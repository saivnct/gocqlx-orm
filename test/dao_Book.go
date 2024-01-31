package test

import (
	cqlxoDAO "giangbb.studio/go.cqlx.orm/dao"
	"github.com/scylladb/gocqlx/v2"
)

type BookDAO struct {
	cqlxoDAO.DAO
}

func mBookDAO(session gocqlx.Session) (*BookDAO, error) {
	d := &BookDAO{}
	err := d.InitDAO(session, Book{})

	return d, err
}
