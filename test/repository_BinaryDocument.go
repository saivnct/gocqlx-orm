package test

import (
	cqlxoRepository "github.com/saivnct/gocqlx-orm/repository"
	"github.com/scylladb/gocqlx/v3"
)

type BinaryDocumentRepository struct {
	cqlxoRepository.BaseScyllaRepository
}

func mBinaryDocumentRepository(session gocqlx.Session) (*BinaryDocumentRepository, error) {
	d := &BinaryDocumentRepository{}
	err := d.InitRepository(session, BinaryDocument{})
	return d, err
}
