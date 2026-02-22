package cqlxoDAO

import (
	"testing"

	cqlxoCodec "github.com/saivnct/gocqlx-orm/codec"
)

type queryEntity struct {
	ID   string
	Name string
	Age  int
}

func (queryEntity) TableName() string { return "query_entity" }

func TestGetQueryMap_SupportsStructAndPointerAndSkipsUnknownColumns(t *testing.T) {
	d := &DAO{
		EntityInfo: cqlxoCodec.EntityInfo{
			ColumFieldMap: map[string]string{
				"id":   "ID",
				"name": "Name",
			},
		},
	}

	resultFromStruct := d.getQueryMap(queryEntity{ID: "1", Name: "alice"}, []string{"id", "name", "unknown"})
	if len(resultFromStruct) != 2 {
		t.Fatalf("expected 2 query filters, got %d", len(resultFromStruct))
	}
	if resultFromStruct["id"] != "1" {
		t.Fatalf("expected id=1, got %v", resultFromStruct["id"])
	}
	if resultFromStruct["name"] != "alice" {
		t.Fatalf("expected name=alice, got %v", resultFromStruct["name"])
	}

	resultFromPointer := d.getQueryMap(&queryEntity{ID: "2", Name: "bob"}, []string{"id", "name"})
	if len(resultFromPointer) != 2 {
		t.Fatalf("expected 2 query filters, got %d", len(resultFromPointer))
	}
	if resultFromPointer["id"] != "2" {
		t.Fatalf("expected id=2, got %v", resultFromPointer["id"])
	}
	if resultFromPointer["name"] != "bob" {
		t.Fatalf("expected name=bob, got %v", resultFromPointer["name"])
	}
}

func TestGetQueryMap_SkipsZeroValues(t *testing.T) {
	d := &DAO{
		EntityInfo: cqlxoCodec.EntityInfo{
			ColumFieldMap: map[string]string{
				"id":  "ID",
				"age": "Age",
			},
		},
	}

	result := d.getQueryMap(queryEntity{ID: "1", Age: 0}, []string{"id", "age"})
	if len(result) != 1 {
		t.Fatalf("expected only non-zero field in query map, got %d", len(result))
	}
	if result["id"] != "1" {
		t.Fatalf("expected id=1, got %v", result["id"])
	}
}
