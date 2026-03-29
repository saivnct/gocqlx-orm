package test

import (
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	cqlxo_connection "github.com/saivnct/gocqlx-orm/connection"
	cqlxoEntity "github.com/saivnct/gocqlx-orm/entity"
	cqlxoRepository "github.com/saivnct/gocqlx-orm/repository"
	"github.com/saivnct/gocqlx-orm/utils/stringUtils"
	"github.com/scylladb/gocqlx/v3"
	"github.com/stretchr/testify/assert"
)

const (
	batchSaveCustomChunkSize      = 50
	batchSaveTotalPackages        = 300
	batchSaveDefaultPathPackages  = 62
	batchSaveFallbackPathPackages = 9
	batchSaveTTLChunkSize         = 50
	batchSaveTTLSeconds           = 30
	batchSaveTotalDocs            = 300
)

func TestExample07_BatchSave_CustomConfig_And_RepositoryFlows(t *testing.T) {
	keyspace := newBatchSaveKeyspace("example_07_batch_cfg")
	err := SetUpKeySpace(keyspace)
	if err != nil {
		t.Fatal(err)
	}

	session := openTestSession(t, keyspace)
	defer func() {
		CleanUp(session, keyspace)
		session.Close()
	}()

	repo, err := mPackageRepository(session)
	if err != nil {
		t.Fatal(err)
	}

	repo.SetBatchSaveConfig(cqlxoRepository.BatchSaveConfig{
		ChunkSize: batchSaveCustomChunkSize,
		Type:      gocql.LoggedBatch,
	})

	entities, byID, names := buildPackagesForBatchSave(batchSaveTotalPackages)
	err = repo.SaveMany(entities)
	assert.NoError(t, err)

	countAll, err := repo.CountAll()
	assert.NoError(t, err)
	assert.Equal(t, int64(batchSaveTotalPackages), countAll)

	var allPackages []*Package
	err = repo.FindAll(&allPackages)
	assert.NoError(t, err)
	assert.Equal(t, batchSaveTotalPackages, len(allPackages))

	firstID := entities[0].(*Package).Id
	var byPK []*Package
	err = repo.FindByPrimaryKey(Package{Id: firstID}, &byPK)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(byPK))
	assert.Equal(t, byID[firstID].Name, byPK[0].Name)

	searchName := names[0]
	var byName []*Package
	err = repo.Find(Package{Name: searchName}, false, &byName)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(byName))
	assert.Equal(t, searchName, byName[0].Name)

	err = repo.DeleteAll()
	assert.NoError(t, err)

	countAll, err = repo.CountAll()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), countAll)
}

func TestExample07_BatchSave_DefaultAndFallbackConfigPaths(t *testing.T) {
	keyspace := newBatchSaveKeyspace("example_07_batch_default")
	err := SetUpKeySpace(keyspace)
	if err != nil {
		t.Fatal(err)
	}

	session := openTestSession(t, keyspace)
	defer func() {
		CleanUp(session, keyspace)
		session.Close()
	}()

	repo, err := mPackageRepository(session)
	if err != nil {
		t.Fatal(err)
	}

	defaultEntities, _, _ := buildPackagesForBatchSave(batchSaveDefaultPathPackages)
	err = repo.SaveMany(defaultEntities)
	assert.NoError(t, err)

	countAll, err := repo.CountAll()
	assert.NoError(t, err)
	assert.Equal(t, int64(batchSaveDefaultPathPackages), countAll)

	err = repo.DeleteAll()
	assert.NoError(t, err)

	repo.SetBatchSaveConfig(cqlxoRepository.BatchSaveConfig{
		ChunkSize: 0,
		Type:      gocql.BatchType(99),
	})

	fallbackEntities, _, _ := buildPackagesForBatchSave(batchSaveFallbackPathPackages)
	err = repo.SaveMany(fallbackEntities)
	assert.NoError(t, err)

	countAll, err = repo.CountAll()
	assert.NoError(t, err)
	assert.Equal(t, int64(batchSaveFallbackPathPackages), countAll)
}

func TestExample07_BatchSaveWithTTL_CustomConfig(t *testing.T) {
	keyspace := newBatchSaveKeyspace("example_07_batch_ttl")
	err := SetUpKeySpace(keyspace)
	if err != nil {
		t.Fatal(err)
	}

	session := openTestSession(t, keyspace)
	defer func() {
		CleanUp(session, keyspace)
		session.Close()
	}()

	repo, err := mBinaryDocumentRepository(session)
	if err != nil {
		t.Fatal(err)
	}

	repo.SetBatchSaveConfig(cqlxoRepository.BatchSaveConfig{
		ChunkSize: batchSaveTTLChunkSize,
		Type:      gocql.LoggedBatch,
	})

	docEntities, docsByID := buildDocsForBatchTTL(batchSaveTotalDocs)
	err = repo.SaveManyWithTTL(docEntities, batchSaveTTLSeconds)
	assert.NoError(t, err)

	countAll, err := repo.CountAll()
	assert.NoError(t, err)
	assert.Equal(t, int64(batchSaveTotalDocs), countAll)

	var docs []BinaryDocument
	err = repo.FindAll(&docs)
	assert.NoError(t, err)
	assert.Equal(t, batchSaveTotalDocs, len(docs))

	for _, doc := range docs {
		original := docsByID[doc.ID]
		assert.Equal(t, original.Name, doc.Name)
		assert.Equal(t, string(original.Payload), string(doc.Payload))
		assert.Equal(t, string(original.Checksum), string(doc.Checksum))
	}

	for _, entity := range docEntities {
		doc, ok := entity.(BinaryDocument)
		assert.True(t, ok)
		var ttlValue int64
		err = session.Session.Query(
			fmt.Sprintf("SELECT TTL(payload) FROM %s WHERE id = ?", BinaryDocument{}.TableName()),
			doc.ID,
		).Scan(&ttlValue)
		assert.NoError(t, err)
		assert.Greater(t, ttlValue, int64(0))
		assert.LessOrEqual(t, ttlValue, int64(batchSaveTTLSeconds))
	}
}

func buildPackagesForBatchSave(total int) ([]cqlxoEntity.BaseScyllaEntityInterface, map[gocql.UUID]*Package, []string) {
	entities := make([]cqlxoEntity.BaseScyllaEntityInterface, 0, total)
	byID := make(map[gocql.UUID]*Package, total)
	names := make([]string, 0, total)

	for i := 0; i < total; i++ {
		name := fmt.Sprintf("pkg-%03d", i)
		pkg := &Package{
			Id:   gocql.TimeUUID(),
			Name: name,
			Tags: []Tag{
				{
					Label:    fmt.Sprintf("label-%03d", i),
					Category: Category{Name: fmt.Sprintf("category-%03d", i), Code: i + 1},
				},
			},
			Origin: GeoPoint{
				Lat:  float64(i) + 10.1,
				Lng:  float64(i) + 20.2,
				Info: Address{Street: fmt.Sprintf("street-%03d", i), City: "city", Zip: 10000 + i},
			},
		}
		entities = append(entities, pkg)
		byID[pkg.Id] = pkg
		names = append(names, name)
	}

	return entities, byID, names
}

func buildDocsForBatchTTL(total int) ([]cqlxoEntity.BaseScyllaEntityInterface, map[gocql.UUID]BinaryDocument) {
	entities := make([]cqlxoEntity.BaseScyllaEntityInterface, 0, total)
	byID := make(map[gocql.UUID]BinaryDocument, total)

	for i := 0; i < total; i++ {
		doc := BinaryDocument{
			ID:       gocql.TimeUUID(),
			Name:     fmt.Sprintf("doc-%03d", i),
			Payload:  []byte(fmt.Sprintf("payload-%03d", i)),
			Checksum: []byte(fmt.Sprintf("checksum-%03d", i)),
		}
		entities = append(entities, doc)
		byID[doc.ID] = doc
	}

	return entities, byID
}

func openTestSession(t *testing.T, keyspace string) gocqlx.Session {
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

	return *sessionP
}

func newBatchSaveKeyspace(prefix string) string {
	return fmt.Sprintf("%s_%s", prefix, stringUtils.GenerateRandomNumberString(6))
}
