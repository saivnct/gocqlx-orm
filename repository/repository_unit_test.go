package cqlxoRepository

import (
	"errors"
	"testing"

	"github.com/gocql/gocql"
	cqlxoCodec "github.com/saivnct/gocqlx-orm/codec"
	cqlxoEntity "github.com/saivnct/gocqlx-orm/entity"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/table"
)

type queryEntity struct {
	ID   string
	Name string
	Age  int
}

func (queryEntity) TableName() string { return "query_entity" }

func TestGetQueryMap_SupportsStructAndPointerAndSkipsUnknownColumns(t *testing.T) {
	d := &BaseScyllaRepository{
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
	d := &BaseScyllaRepository{
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

func TestSaveWithTTL_RejectsInvalidTTL(t *testing.T) {
	d := &BaseScyllaRepository{}

	err := d.SaveWithTTL(queryEntity{}, 0)
	if !errors.Is(err, InvalidTTL) {
		t.Fatalf("expected InvalidTTL, got %v", err)
	}
}

func TestSaveManyWithTTL_RejectsInvalidTTL(t *testing.T) {
	d := &BaseScyllaRepository{}

	err := d.SaveManyWithTTL(nil, -1)
	if !errors.Is(err, InvalidTTL) {
		t.Fatalf("expected InvalidTTL, got %v", err)
	}
}

func TestGetInsertStmtWithTTL_AppendsUsingTTL(t *testing.T) {
	d := &BaseScyllaRepository{
		EntityInfo: cqlxoCodec.EntityInfo{
			TableMetaData: table.Metadata{
				Name:    "test_entity",
				Columns: []string{"id", "name"},
			},
		},
	}

	stmt := d.getInsertStmtWithTTL()
	expected := "INSERT INTO test_entity (id, name) VALUES (?, ?) USING TTL ?"
	if stmt != expected {
		t.Fatalf("unexpected statement:\n got: %s\nwant: %s", stmt, expected)
	}
}

func TestBatchConfig_FallbackToDefaultsWhenInvalid(t *testing.T) {
	d := newBatchTestRepository()
	d.SetBatchSaveConfig(BatchSaveConfig{
		ChunkSize: 0,
		Type:      gocql.BatchType(99),
	})

	if size := d.getBatchChunkSize(); size != defaultBatchChunkSize {
		t.Fatalf("expected default chunk size %d, got %d", defaultBatchChunkSize, size)
	}
	if batchType := d.getBatchType(); batchType != defaultBatchType {
		t.Fatalf("expected default batch type %v, got %v", defaultBatchType, batchType)
	}
}

func TestSaveMany_WithoutSessionReturnsNoSessionError(t *testing.T) {
	d := &BaseScyllaRepository{}

	err := d.SaveMany(makeQueryEntities(1))
	if !errors.Is(err, NoSessionError) {
		t.Fatalf("expected NoSessionError, got %v", err)
	}
}

func newBatchTestRepository() *BaseScyllaRepository {
	return &BaseScyllaRepository{
		EntityInfo: cqlxoCodec.EntityInfo{
			TableMetaData: table.Metadata{
				Name:    "query_entity",
				Columns: []string{"id", "name"},
			},
			Columns: []cqlxoCodec.ColumnInfo{
				{Name: "id", Type: gocql.NewNativeType(0, gocql.TypeText)},
				{Name: "name", Type: gocql.NewNativeType(0, gocql.TypeText)},
			},
			ColumFieldMap: map[string]string{
				"id":   "ID",
				"name": "Name",
			},
		},
		Session: gocqlx.Session{
			Session: &gocql.Session{},
		},
	}
}

func makeQueryEntities(n int) []cqlxoEntity.BaseScyllaEntityInterface {
	entities := make([]cqlxoEntity.BaseScyllaEntityInterface, 0, n)
	for i := 0; i < n; i++ {
		entities = append(entities, &queryEntity{
			ID:   "id",
			Name: "name",
		})
	}
	return entities
}
