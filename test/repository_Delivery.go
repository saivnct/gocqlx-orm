package test

import (
	cqlxoDAO "github.com/saivnct/gocqlx-orm/repository"
	"github.com/scylladb/gocqlx/v3"
)

type DeliveryRepository struct {
	cqlxoDAO.BaseScyllaRepository
}

func mDeliveryRepository(session gocqlx.Session) (*DeliveryRepository, error) {
	d := &DeliveryRepository{}
	err := d.InitRepository(session, Delivery{})
	return d, err
}
