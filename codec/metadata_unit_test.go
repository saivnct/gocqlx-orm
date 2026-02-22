package cqlxoCodec

import (
	"errors"
	"reflect"
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
)

type duplicatePKEntity struct {
	ID    gocql.UUID `db:"id" pk:"1"`
	Email string     `db:"email" pk:"1"`
}

func (duplicatePKEntity) TableName() string { return "duplicate_pk_entity" }

type duplicateCKEntity struct {
	P1 gocql.UUID `db:"p1" pk:"1"`
	C1 int        `db:"c1" ck:"1"`
	C2 int        `db:"c2" ck:"1"`
}

func (duplicateCKEntity) TableName() string { return "duplicate_ck_entity" }

type pointTuple struct {
	Lat float64 `db:"lat"`
	Lng float64 `db:"lng"`
}

func (pointTuple) Tuple() string { return "point_tuple" }

type tupleEntity struct {
	ID    gocql.UUID `db:"id" pk:"1"`
	Point pointTuple `db:"point"`
}

func (tupleEntity) TableName() string { return "tuple_entity" }

type innerUDT struct {
	gocqlx.UDT
	Value string `db:"value"`
}

type outerUDT struct {
	gocqlx.UDT
	Inner innerUDT `db:"inner"`
}

type udtCollectionsEntity struct {
	ID      gocql.UUID   `db:"id" pk:"1"`
	History []innerUDT   `db:"history"`
	Current outerUDT     `db:"current"`
	Matrix  [][]outerUDT `db:"matrix"`
}

func (udtCollectionsEntity) TableName() string { return "udt_collections_entity" }

func TestParseTableMetaData_DuplicatePartitionKeyIndex(t *testing.T) {
	_, err := ParseTableMetaData(duplicatePKEntity{})
	if !errors.Is(err, InvalidPartitionKeyErr) {
		t.Fatalf("expected InvalidPartitionKeyErr, got %v", err)
	}
}

func TestParseTableMetaData_DuplicateClusterKeyIndex(t *testing.T) {
	_, err := ParseTableMetaData(duplicateCKEntity{})
	if !errors.Is(err, InvalidClusterKeyErr) {
		t.Fatalf("expected InvalidClusterKeyErr, got %v", err)
	}
}

func TestParseTableMetaData_SupportsTupleViaBaseTupleEmbedding(t *testing.T) {
	entityInfo, err := ParseTableMetaData(tupleEntity{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(entityInfo.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(entityInfo.Columns))
	}

	if entityInfo.Columns[1].Name != "point" {
		t.Fatalf("expected point column, got %s", entityInfo.Columns[1].Name)
	}

	if entityInfo.Columns[1].Type.Type() != gocql.TypeTuple {
		t.Fatalf("expected tuple type, got %s", entityInfo.Columns[1].Type.Type())
	}
}

func TestGetCqlType_MapWithoutWhitespaceDelimiter(t *testing.T) {
	cqlType, err := getCqlType("map<text,int>")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	collection, ok := cqlType.(gocql.CollectionType)
	if !ok {
		t.Fatalf("expected map collection type, got %T", cqlType)
	}
	if collection.Type() != gocql.TypeMap {
		t.Fatalf("expected map type, got %s", collection.Type())
	}
	if collection.Key.Type() != gocql.TypeText {
		t.Fatalf("expected text key type, got %s", collection.Key.Type())
	}
	if collection.Elem.Type() != gocql.TypeInt {
		t.Fatalf("expected int value type, got %s", collection.Elem.Type())
	}
}

func TestGetCqlCreateUDTStatement_NestedUDTFieldIsFrozen(t *testing.T) {
	cqlType, err := convertToDefaultCqlType(reflect.TypeOf(outerUDT{}))
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	cqlUDT, ok := cqlType.(gocql.UDTTypeInfo)
	if !ok {
		t.Fatalf("expected UDT type, got %T", cqlType)
	}

	stmt := GetCqlCreateUDTStatement(cqlUDT)
	if stmt != "CREATE TYPE IF NOT EXISTS outer_udt (inner frozen<inner_udt>)" {
		t.Fatalf("unexpected statement: %s", stmt)
	}
}

func TestParseTableMetaData_SupportsUDTCollectionsAndNestedUDTScan(t *testing.T) {
	entityInfo, err := ParseTableMetaData(udtCollectionsEntity{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	columnDecls := map[string]string{}
	for _, col := range entityInfo.Columns {
		columnDecls[col.Name] = col.GetCqlTypeDeclareStatement()
	}

	if columnDecls["history"] != "history list<frozen<inner_udt>>" {
		t.Fatalf("unexpected history declaration: %s", columnDecls["history"])
	}
	if columnDecls["current"] != "current frozen<outer_udt>" {
		t.Fatalf("unexpected current declaration: %s", columnDecls["current"])
	}
	if columnDecls["matrix"] != "matrix list<frozen<list<frozen<outer_udt>>>>" {
		t.Fatalf("unexpected matrix declaration: %s", columnDecls["matrix"])
	}

	udts := entityInfo.ScanUDTs()
	if len(udts) < 2 {
		t.Fatalf("expected at least 2 udts, got %d", len(udts))
	}

	containsInner := false
	containsOuter := false
	for _, udt := range udts {
		if udt.Name == "inner_udt" {
			containsInner = true
		}
		if udt.Name == "outer_udt" {
			containsOuter = true
		}
	}
	if !containsInner || !containsOuter {
		t.Fatalf("expected both inner_udt and outer_udt to be scanned, got: %+v", udts)
	}
}
