package test

import "github.com/scylladb/gocqlx/v3"

// Tag is a UDT that itself contains a nested UDT (Category).
type Tag struct {
	gocqlx.UDT
	Label    string   `db:"label"`
	Category Category `db:"category"`
}
