package test

import "github.com/gocql/gocql"

// Package is an entity that exercises:
//   - []Tag  : slice of a nested UDT (Tag contains a Category UDT)
//   - GeoPoint : tuple whose elements include a UDT (Address)
type Package struct {
	Id     gocql.UUID `db:"id" pk:"1"`
	Name   string     `db:"name" index:"true"`
	Tags   []Tag      `db:"tags"`
	Origin GeoPoint   `db:"origin"`
}

func (Package) TableName() string {
	return "package"
}
