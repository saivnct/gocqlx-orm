package test

import (
	"github.com/saivnct/gocqlx-orm/repository"
	"github.com/scylladb/gocqlx/v3"
)

type BookRepository struct {
	cqlxoRepository.BaseScyllaRepository
}

func mBookRepository(session gocqlx.Session) (*BookRepository, error) {
	d := &BookRepository{}
	err := d.InitRepository(session, Book{})

	return d, err
}
