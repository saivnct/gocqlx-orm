package test

import (
	cqlxoDAO "github.com/saivnct/gocqlx-orm/dao"
	"github.com/scylladb/gocqlx/v3"
)

type PackageDAO struct {
	cqlxoDAO.DAO
}

func mPackageDAO(session gocqlx.Session) (*PackageDAO, error) {
	d := &PackageDAO{}
	err := d.InitDAO(session, Package{})
	return d, err
}
