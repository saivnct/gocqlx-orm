package test

import (
	cqlxoDAO "github.com/saivnct/gocqlx-orm/dao"
	"github.com/scylladb/gocqlx/v3"
)

type DeliveryDAO struct {
	cqlxoDAO.DAO
}

func mDeliveryDAO(session gocqlx.Session) (*DeliveryDAO, error) {
	d := &DeliveryDAO{}
	err := d.InitDAO(session, Delivery{})
	return d, err
}
