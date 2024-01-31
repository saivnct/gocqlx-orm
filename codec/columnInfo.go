package cqlxoCodec

import (
	"fmt"
	"github.com/gocql/gocql"
)

type ColumnInfo struct {
	Name   string
	Frozen bool
	Type   gocql.TypeInfo
}

func (c ColumnInfo) String() string {
	return fmt.Sprintf("[%s] - %s", c.Name, GetCqlTypeInfo(c.Type))
}

func (c ColumnInfo) GetCqlTypeDeclareStatement() string {
	if c.Frozen {
		return fmt.Sprintf("%s frozen<%s>", c.Name, GetCqlTypeDeclareStatement(c.Type, false))
	}
	return fmt.Sprintf("%s %s", c.Name, GetCqlTypeDeclareStatement(c.Type, false))
}
