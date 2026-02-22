package test

import "github.com/scylladb/gocqlx/v3"

type DeliveryProfile struct {
	gocqlx.UDT
	PrimaryAddress Address   `db:"primary_address"`
	AddressHistory []Address `db:"address_history"`
}
