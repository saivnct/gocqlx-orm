package cqlxoCodec

import (
	"fmt"
	"giangbb.studio/go.cqlx.orm/utils/sliceUtils"
	"github.com/gocql/gocql"
	"strings"
)

type EntityInfo struct {
	TableName   string
	ColumnsMap  map[string]ColumnInfo
	Columns     []ColumnInfo
	ColumnsName []string
	PartKey     []string
	SortKey     []string
}

func (e EntityInfo) GetPrimaryKey() string {

	pk := e.PartKey[0]
	if len(e.PartKey) > 1 {
		pk = fmt.Sprintf("(%s)", strings.Join(e.PartKey, ", "))
	}

	if len(e.SortKey) > 0 {
		return fmt.Sprintf("%s, %s", pk, strings.Join(e.SortKey, ", "))
	} else {
		return pk
	}
}

func (e EntityInfo) GetGreateTableStatement() string {
	colStms := sliceUtils.Map(e.Columns, func(c ColumnInfo) string { return c.GetCqlTypeDeclareStatement() })
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s , PRIMARY KEY (%s))",
		e.TableName,
		strings.Join(colStms, ", "),
		e.GetPrimaryKey())
}

func (e EntityInfo) ScanUDTs() []gocql.UDTTypeInfo {
	var udtColInfo []gocql.UDTTypeInfo
	for _, column := range e.Columns {
		udtColInfo = append(udtColInfo, ScanUDTs(column.Type)...)
	}
	return udtColInfo
}
