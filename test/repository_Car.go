package test

import (
	"github.com/saivnct/gocqlx-orm/repository"
	"github.com/scylladb/gocqlx/v3"
)

type CarRepository struct {
	cqlxoRepository.BaseScyllaRepository
}

func mCarRepository(session gocqlx.Session) (*CarRepository, error) {
	d := &CarRepository{}
	err := d.InitRepository(session, Car{})
	return d, err
}
