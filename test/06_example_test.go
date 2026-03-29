package test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	cqlxo_connection "github.com/saivnct/gocqlx-orm/connection"
	cqlxoEntity "github.com/saivnct/gocqlx-orm/entity"
	"github.com/stretchr/testify/assert"
)

func TestExample06_ByteArrayField(t *testing.T) {
	keyspace := "example_06"

	err := SetUpKeySpace(keyspace)
	if err != nil {
		t.Fatal(err)
	}

	_, sessionP, err := cqlxo_connection.CreateCluster(
		hosts,
		"cassandra",
		"",
		keyspace,
		gocql.ParseConsistency(consistencyLV),
		gocql.ParseConsistency(serialConsistencyLV),
		localDC,
		clusterTimeout,
		numRetries,
	)
	if err != nil {
		t.Fatal(err)
	}
	session := *sessionP
	defer func() {
		CleanUp(session, keyspace)
		session.Close()
	}()

	repo, err := mBinaryDocumentRepository(session)
	if err != nil {
		t.Fatal(err)
	}

	expectedCols := map[string]string{
		"id":       "id uuid",
		"name":     "name text",
		"payload":  "payload blob",
		"checksum": "checksum blob",
	}
	assert.Equal(t, len(expectedCols), len(repo.EntityInfo.Columns))
	for _, col := range repo.EntityInfo.Columns {
		want, ok := expectedCols[col.Name]
		assert.True(t, ok, "unexpected column %q", col.Name)
		assert.Equal(t, want, col.GetCqlTypeDeclareStatement())
	}

	for _, index := range repo.EntityInfo.Indexes {
		var count int
		indexName := fmt.Sprintf("%s_%s_idx", repo.EntityInfo.TableMetaData.Name, index)
		err = session.Query(
			fmt.Sprintf("SELECT COUNT(*) FROM system_schema.indexes WHERE keyspace_name = '%s' AND table_name = '%s' AND index_name = '%s'",
				keyspace, repo.EntityInfo.TableMetaData.Name, indexName),
			nil,
		).Get(&count)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	}

	doc1 := BinaryDocument{
		ID:       gocql.TimeUUID(),
		Name:     "file-a",
		Payload:  []byte{0, 1, 2, 3, 4, 255},
		Checksum: []byte{10, 20, 30, 40},
	}
	doc2 := BinaryDocument{
		ID:       gocql.TimeUUID(),
		Name:     "file-b",
		Payload:  []byte("hello-scylla"),
		Checksum: nil,
	}

	err = repo.Save(doc1)
	assert.NoError(t, err)

	err = repo.Save(doc2)
	assert.NoError(t, err)

	countAll, err := repo.CountAll()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), countAll)

	var byPK []BinaryDocument
	err = repo.FindByPrimaryKey(BinaryDocument{ID: doc1.ID}, &byPK)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(byPK))
	assert.Equal(t, doc1.Name, byPK[0].Name)
	assert.True(t, bytes.Equal(doc1.Payload, byPK[0].Payload), "payload mismatch for doc1")
	assert.True(t, bytes.Equal(doc1.Checksum, byPK[0].Checksum), "checksum mismatch for doc1")

	byPK = nil
	err = repo.FindByPrimaryKey(BinaryDocument{ID: doc2.ID}, &byPK)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(byPK))
	assert.Equal(t, doc2.Name, byPK[0].Name)
	assert.True(t, bytes.Equal(doc2.Payload, byPK[0].Payload), "payload mismatch for doc2")
	assert.Equal(t, 0, len(byPK[0].Checksum), "checksum should round-trip as empty/nil")

	var all []BinaryDocument
	err = repo.FindAll(&all)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(all))

	err = repo.DeleteAll()
	assert.NoError(t, err)
	countAll, err = repo.CountAll()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), countAll)
}

func TestExample06_SaveWithTTL_SaveManyWithTTL(t *testing.T) {
	keyspace := "example_06_ttl"

	err := SetUpKeySpace(keyspace)
	if err != nil {
		t.Fatal(err)
	}

	_, sessionP, err := cqlxo_connection.CreateCluster(
		hosts,
		"cassandra",
		"",
		keyspace,
		gocql.ParseConsistency(consistencyLV),
		gocql.ParseConsistency(serialConsistencyLV),
		localDC,
		clusterTimeout,
		numRetries,
	)
	if err != nil {
		t.Fatal(err)
	}
	session := *sessionP
	defer func() {
		CleanUp(session, keyspace)
		session.Close()
	}()

	repo, err := mBinaryDocumentRepository(session)
	if err != nil {
		t.Fatal(err)
	}

	doc1 := BinaryDocument{
		ID:       gocql.TimeUUID(),
		Name:     "ttl-single",
		Payload:  []byte("will-expire"),
		Checksum: []byte{1, 2, 3},
	}
	doc2 := BinaryDocument{
		ID:       gocql.TimeUUID(),
		Name:     "ttl-many-1",
		Payload:  []byte{7, 8, 9},
		Checksum: []byte{10, 11},
	}
	doc3 := BinaryDocument{
		ID:       gocql.TimeUUID(),
		Name:     "ttl-many-2",
		Payload:  []byte{20, 21, 22},
		Checksum: []byte{30, 31},
	}

	var ttlSingle int64 = 3 // seconds
	var ttlMany int64 = 5   // seconds
	err = repo.SaveWithTTL(doc1, ttlSingle)
	assert.NoError(t, err)

	err = repo.SaveManyWithTTL([]cqlxoEntity.BaseScyllaEntityInterface{doc2, doc3}, ttlMany)
	assert.NoError(t, err)

	var ttlValue int64
	err = session.Session.Query("SELECT TTL(payload) FROM binary_document WHERE id = ?", doc1.ID).Scan(&ttlValue)
	assert.NoError(t, err)
	assert.Equal(t, ttlValue, ttlSingle)
	t.Logf("doc1 ttl: %d", ttlValue)

	err = session.Session.Query("SELECT TTL(payload) FROM binary_document WHERE id = ?", doc2.ID).Scan(&ttlValue)
	assert.NoError(t, err)
	assert.Equal(t, ttlValue, ttlMany)
	t.Logf("doc2 ttl: %d", ttlValue)

	err = session.Session.Query("SELECT TTL(payload) FROM binary_document WHERE id = ?", doc3.ID).Scan(&ttlValue)
	assert.NoError(t, err)
	assert.Equal(t, ttlValue, ttlMany)
	t.Logf("doc3 ttl: %d", ttlValue)

	assert.Eventually(t, func() bool {
		countAll, e := repo.CountAll()
		return e == nil && countAll == 0
	}, 10*time.Second, 500*time.Millisecond, "expected all TTL rows to expire")
}
