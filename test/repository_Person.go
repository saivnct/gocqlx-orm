package test

import (
	"github.com/saivnct/gocqlx-orm/repository"
	"github.com/scylladb/gocqlx/v3"
)

type PersonRepository struct {
	cqlxoRepository.BaseScyllaRepository
}

func mPersonRepository(session gocqlx.Session) (*PersonRepository, error) {
	d := &PersonRepository{}
	err := d.InitRepository(session, Person{})

	return d, err
}
