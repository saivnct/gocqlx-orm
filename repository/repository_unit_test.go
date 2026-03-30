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

func TestDeleteManyByPrimaryKey_UsesBatchExecutionWithDefaultConfig(t *testing.T) {
	d := newBatchTestRepository()
	var executedBatches []*gocql.Batch

	d.newBatchFn = func(batchType gocql.BatchType) *gocql.Batch {
		return &gocql.Batch{Type: batchType}
	}
	d.executeBatchFn = func(batch *gocql.Batch) error {
		executedBatches = append(executedBatches, batch)
		return nil
	}

	entities := makeQueryEntities(120)
	if err := d.DeleteManyByPrimaryKey(entities); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(executedBatches) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(executedBatches))
	}

	expectedStmt := "DELETE FROM query_entity WHERE id = ? AND name = ?"
	expectedSizes := []int{50, 50, 20}
	for i, batch := range executedBatches {
		if batch.Type != gocql.UnloggedBatch {
			t.Fatalf("expected default unlogged batch type, got %v", batch.Type)
		}
		if len(batch.Entries) != expectedSizes[i] {
			t.Fatalf("batch %d expected %d entries, got %d", i, expectedSizes[i], len(batch.Entries))
		}
		for _, entry := range batch.Entries {
			if entry.Stmt != expectedStmt {
				t.Fatalf("unexpected stmt, got %q want %q", entry.Stmt, expectedStmt)
			}
			if len(entry.Args) != 2 {
				t.Fatalf("expected 2 args for primary-key delete, got %d", len(entry.Args))
			}
		}
	}
}

func TestDeleteManyByPrimaryKey_UsesCustomBatchConfig(t *testing.T) {
	d := newBatchTestRepository()
	var executedBatches []*gocql.Batch

	d.SetBatchSaveConfig(BatchSaveConfig{
		ChunkSize: 2,
		Type:      gocql.LoggedBatch,
	})
	d.newBatchFn = func(batchType gocql.BatchType) *gocql.Batch {
		return &gocql.Batch{Type: batchType}
	}
	d.executeBatchFn = func(batch *gocql.Batch) error {
		executedBatches = append(executedBatches, batch)
		return nil
	}

	if err := d.DeleteManyByPrimaryKey(makeQueryEntities(5)); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(executedBatches) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(executedBatches))
	}
	for _, batch := range executedBatches {
		if batch.Type != gocql.LoggedBatch {
			t.Fatalf("expected logged batch type, got %v", batch.Type)
		}
	}
}

func TestDeleteManyByPrimaryKey_RejectsInvalidPrimaryKey(t *testing.T) {
	d := newBatchTestRepository()
	d.newBatchFn = func(batchType gocql.BatchType) *gocql.Batch {
		return &gocql.Batch{Type: batchType}
	}
	d.executeBatchFn = func(batch *gocql.Batch) error {
		return nil
	}

	err := d.DeleteManyByPrimaryKey([]cqlxoEntity.BaseScyllaEntityInterface{
		queryEntity{ID: "id-only"},
	})
	if !errors.Is(err, InvalidPrimaryKey) {
		t.Fatalf("expected InvalidPrimaryKey, got %v", err)
	}
}

func TestDeleteManyByPrimaryKey_EmptyInputDoesNotExecuteBatch(t *testing.T) {
	d := newBatchTestRepository()
	executed := false

	d.newBatchFn = func(batchType gocql.BatchType) *gocql.Batch {
		return &gocql.Batch{Type: batchType}
	}
	d.executeBatchFn = func(batch *gocql.Batch) error {
		executed = true
		return nil
	}

	if err := d.DeleteManyByPrimaryKey(nil); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if executed {
		t.Fatalf("expected no batch execution for empty input")
	}
}

func TestDeleteManyByPrimaryKey_WithoutSessionReturnsNoSessionError(t *testing.T) {
	d := &BaseScyllaRepository{}

	err := d.DeleteManyByPrimaryKey(makeQueryEntities(1))
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
				PartKey: []string{"id"},
				SortKey: []string{"name"},
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
