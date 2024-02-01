package cqlxoCodec

import (
	"fmt"
	"giangbb.studio/go.cqlx.orm/utils/sliceUtils"
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2/table"
	"strings"
)

type EntityInfo struct {
	TableMetaData  table.Metadata
	Table          *table.Table
	Columns        []ColumnInfo
	Indexes        []string
	ColumFieldMap  map[string]string //column name => field name
	FieldColumdMap map[string]string //field name => column name
}

func (e EntityInfo) GetPrimaryKey() string {

	pk := e.TableMetaData.PartKey[0]
	if len(e.TableMetaData.PartKey) > 1 {
		pk = fmt.Sprintf("(%s)", strings.Join(e.TableMetaData.PartKey, ", "))
	}

	if len(e.TableMetaData.SortKey) > 0 {
		return fmt.Sprintf("%s, %s", pk, strings.Join(e.TableMetaData.SortKey, ", "))
	} else {
		return pk
	}
}

func (e EntityInfo) GetGreateTableStatement() string {
	colStms := sliceUtils.Map(e.Columns, func(c ColumnInfo) string { return c.GetCqlTypeDeclareStatement() })
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s , PRIMARY KEY (%s))",
		e.TableMetaData.Name,
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
