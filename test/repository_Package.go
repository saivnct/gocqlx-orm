package test

import (
	cqlxoDAO "github.com/saivnct/gocqlx-orm/repository"
	"github.com/scylladb/gocqlx/v3"
)

type PackageRepository struct {
	cqlxoDAO.BaseScyllaRepository
}

func mPackageRepository(session gocqlx.Session) (*PackageRepository, error) {
	d := &PackageRepository{}
	err := d.InitRepository(session, Package{})
	return d, err
}
