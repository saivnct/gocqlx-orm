package test

import (
	"fmt"
	"log"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/gocql/gocql"
	cqlxoCodec "github.com/saivnct/gocqlx-orm/codec"
	cqlxo_connection "github.com/saivnct/gocqlx-orm/connection"
	cqlxoEntity "github.com/saivnct/gocqlx-orm/entity"
	cqlxoDAO "github.com/saivnct/gocqlx-orm/repository"
	"github.com/stretchr/testify/assert"
)

// TestExample05 covers two advanced ORM features:
//
//  1. Slice of nested UDT of nested UDT
//     Package.Tags []Tag   — Tag(UDT) contains Category(UDT)
//
//  2. Tuple of UDT
//     Package.Origin GeoPoint  — GeoPoint(Tuple) contains Address(UDT) as an element
func TestExample05_SliceNestedUDT_TupleOfUDT(t *testing.T) {
	keyspace := "example_05"

	err := SetUpKeySpace(keyspace)
	if err != nil {
		t.Fatal(err)
	}

	_, sessionP, err := cqlxo_connection.CreateCluster(hosts, "cassandra", "", keyspace, gocql.ParseConsistency(consistencyLV), localDC, clusterTimeout, numRetries)
	if err != nil {
		t.Fatal(err)
	}
	session := *sessionP
	defer func() {
		CleanUp(session, keyspace)
		session.Close()
	}()

	packageRepository, err := mPackageRepository(session)
	if err != nil {
		t.Fatal(err)
	}

	// -----------------------------------------------------------------------
	// 1. Verify parsed column declarations
	// -----------------------------------------------------------------------

	// tags  : list of frozen<tag>  (Tag is a UDT → frozen inside list)
	// origin: tuple<double, double, frozen<address>>  (GeoPoint tuple with UDT element)
	expectedCols := map[string]string{
		"id":     "id uuid",
		"name":   "name text",
		"tags":   "tags list<frozen<tag>>",
		"origin": "origin tuple<double, double, frozen<address>>",
	}

	assert.Equal(t, len(packageRepository.EntityInfo.Columns), len(expectedCols),
		"unexpected number of columns: got %d want %d",
		len(packageRepository.EntityInfo.Columns), len(expectedCols))

	for _, c := range packageRepository.EntityInfo.Columns {
		want, ok := expectedCols[c.Name]
		assert.True(t, ok, "unexpected column %q", c.Name)
		assert.Equal(t, want, c.GetCqlTypeDeclareStatement(),
			"column %q declaration mismatch: got %q want %q",
			c.Name, c.GetCqlTypeDeclareStatement(), want)
	}

	// -----------------------------------------------------------------------
	// 2. Verify CREATE TABLE statement
	// -----------------------------------------------------------------------
	t.Logf("CREATE TABLE: %s", packageRepository.EntityInfo.GetCreateTableStatement())

	var tableCount int
	err = session.Query(
		fmt.Sprintf("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s'",
			keyspace, Package{}.TableName()),
		nil,
	).Get(&tableCount)
	assert.Nil(t, err)
	assert.Equal(t, 1, tableCount, "table %q should exist", Package{}.TableName())

	// -----------------------------------------------------------------------
	// 3. Verify UDT detection and CREATE TYPE statements
	//
	//    ScanUDTs walks all column types recursively, so for Package we expect:
	//      - "tag"      (from list<frozen<tag>>)
	//      - "category" (nested inside tag)
	//      - "address"  (element inside the GeoPoint tuple)
	// -----------------------------------------------------------------------
	expectedUDTStatements := map[string]string{
		"category": "CREATE TYPE IF NOT EXISTS category (name text, code int)",
		"tag":      "CREATE TYPE IF NOT EXISTS tag (label text, category frozen<category>)",
		"address":  "CREATE TYPE IF NOT EXISTS address (street text, city text, zip int)",
	}

	udts := packageRepository.EntityInfo.ScanUDTs()
	assert.Equal(t, len(expectedUDTStatements), len(udts),
		"unexpected number of UDTs: got %d want %d", len(udts), len(expectedUDTStatements))

	for _, udt := range udts {
		want, ok := expectedUDTStatements[udt.Name]
		assert.True(t, ok, "unexpected UDT %q", udt.Name)

		got := cqlxoCodec.GetCqlCreateUDTStatement(udt)
		assert.Equal(t, want, got,
			"UDT %q statement mismatch:\n  got  %q\n  want %q", udt.Name, got, want)

		t.Logf("UDT statement: %s", got)
	}

	// Verify every expected UDT was actually created in the keyspace
	for udtName := range expectedUDTStatements {
		var count int
		err = session.Query(
			fmt.Sprintf("SELECT COUNT(*) FROM system_schema.types WHERE keyspace_name = '%s' AND type_name = '%s'",
				keyspace, udtName),
			nil,
		).Get(&count)
		assert.Nil(t, err)
		assert.Equal(t, 1, count, "UDT %q should exist in keyspace %q", udtName, keyspace)
	}

	// -----------------------------------------------------------------------
	// 4. Verify index creation for "name"
	// -----------------------------------------------------------------------
	for _, index := range packageRepository.EntityInfo.Indexes {
		var count int
		indexName := fmt.Sprintf("%s_%s_idx", packageRepository.EntityInfo.TableMetaData.Name, index)
		t.Logf("Index: %s", indexName)
		err = session.Query(
			fmt.Sprintf("SELECT COUNT(*) FROM system_schema.indexes WHERE keyspace_name = '%s' AND table_name = '%s' AND index_name = '%s'",
				keyspace, packageRepository.EntityInfo.TableMetaData.Name, indexName),
			nil,
		).Get(&count)
		assert.Nil(t, err)
		assert.Equal(t, 1, count, "index %q should exist", indexName)
	}

	// -----------------------------------------------------------------------
	// 5. CRUD — Save, FindAll, FindByPrimaryKey, DeleteAll
	// -----------------------------------------------------------------------

	pkg1 := &Package{
		Id:   gocql.TimeUUID(),
		Name: "pkg-alpha",
		Tags: []Tag{
			{
				Label:    "electronics",
				Category: Category{Name: "tech", Code: 1},
			},
			{
				Label:    "gadget",
				Category: Category{Name: "device", Code: 2},
			},
		},
		Origin: GeoPoint{
			Lat:  37.7749,
			Lng:  -122.4194,
			Info: Address{Street: "1 Market St", City: "San Francisco", Zip: 94105},
		},
	}

	pkg2 := &Package{
		Id:   gocql.TimeUUID(),
		Name: "pkg-beta",
		Tags: []Tag{
			{
				Label:    "clothing",
				Category: Category{Name: "fashion", Code: 3},
			},
		},
		Origin: GeoPoint{
			Lat:  40.7128,
			Lng:  -74.0060,
			Info: Address{Street: "5 Ave", City: "New York", Zip: 10001},
		},
	}

	err = packageRepository.Save(pkg1)
	assert.Nil(t, err, "Save pkg1 failed")

	err = packageRepository.Save(pkg2)
	assert.Nil(t, err, "Save pkg2 failed")

	// CountAll
	countAll, err := packageRepository.CountAll()
	assert.Nil(t, err)
	assert.Equal(t, int64(2), countAll, "expected 2 rows, got %d", countAll)

	// FindAll
	var packages []*Package
	err = packageRepository.FindAll(&packages)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(packages), "FindAll should return 2 rows, got %d", len(packages))

	// FindByPrimaryKey — pkg1
	findWithPrimKey := func(packageDAO *PackageRepository, id gocql.UUID) (*Package, error) {
		var results []*Package
		err = packageDAO.FindByPrimaryKey(Package{
			Id: id,
		}, &results)

		if err != nil {
			return nil, err
		}
		if len(results) == 0 {
			return nil, nil
		}
		return results[0], nil
	}

	got1, err := findWithPrimKey(packageRepository, pkg1.Id)
	assert.Nil(t, err)
	assert.NotNil(t, got1)
	spew.Dump(got1)

	assert.Equal(t, pkg1.Id, got1.Id, "Id mismatch")
	assert.Equal(t, pkg1.Name, got1.Name, "Name mismatch")

	// Verify slice of nested UDT round-trips correctly
	assert.Equal(t, len(pkg1.Tags), len(got1.Tags),
		"Tags length mismatch: got %d want %d", len(got1.Tags), len(pkg1.Tags))
	for i, tag := range got1.Tags {
		assert.Equal(t, pkg1.Tags[i].Label, tag.Label,
			"Tags[%d].Label mismatch: got %q want %q", i, tag.Label, pkg1.Tags[i].Label)
		assert.Equal(t, pkg1.Tags[i].Category.Name, tag.Category.Name,
			"Tags[%d].Category.Name mismatch: got %q want %q", i, tag.Category.Name, pkg1.Tags[i].Category.Name)
		assert.Equal(t, pkg1.Tags[i].Category.Code, tag.Category.Code,
			"Tags[%d].Category.Code mismatch: got %d want %d", i, tag.Category.Code, pkg1.Tags[i].Category.Code)
	}

	// Verify tuple of UDT round-trips correctly
	assert.Equal(t, pkg1.Origin.Lat, got1.Origin.Lat, "Origin.Lat mismatch")
	assert.Equal(t, pkg1.Origin.Lng, got1.Origin.Lng, "Origin.Lng mismatch")
	assert.Equal(t, pkg1.Origin.Info.Street, got1.Origin.Info.Street, "Origin.Info.Street mismatch")
	assert.Equal(t, pkg1.Origin.Info.City, got1.Origin.Info.City, "Origin.Info.City mismatch")
	assert.Equal(t, pkg1.Origin.Info.Zip, got1.Origin.Info.Zip, "Origin.Info.Zip mismatch")

	// FindByPrimaryKey — pkg2
	got2, err := findWithPrimKey(packageRepository, pkg2.Id)
	assert.Nil(t, err)
	assert.NotNil(t, got1)
	spew.Dump(got2)
	assert.Equal(t, pkg2.Id, got2.Id)
	assert.Equal(t, 1, len(got2.Tags), "pkg2 Tags length mismatch")
	for i, tag := range got2.Tags {
		assert.Equal(t, pkg2.Tags[i].Label, tag.Label,
			"Tags[%d].Label mismatch: got %q want %q", i, tag.Label, pkg2.Tags[i].Label)
		assert.Equal(t, pkg2.Tags[i].Category.Name, tag.Category.Name,
			"Tags[%d].Category.Name mismatch: got %q want %q", i, tag.Category.Name, pkg2.Tags[i].Category.Name)
		assert.Equal(t, pkg2.Tags[i].Category.Code, tag.Category.Code,
			"Tags[%d].Category.Code mismatch: got %d want %d", i, tag.Category.Code, pkg2.Tags[i].Category.Code)
	}

	assert.Equal(t, pkg2.Origin.Lat, got2.Origin.Lat)
	assert.Equal(t, pkg2.Origin.Lng, got2.Origin.Lng)
	assert.Equal(t, pkg2.Origin.Info.Street, got2.Origin.Info.Street, "Origin.Info.Street mismatch")
	assert.Equal(t, pkg2.Origin.Info.City, got2.Origin.Info.City, "Origin.Info.City mismatch")
	assert.Equal(t, pkg2.Origin.Info.Zip, got2.Origin.Info.Zip, "Origin.Info.Zip mismatch")

	// DeleteAll
	err = packageRepository.DeleteAll()
	assert.Nil(t, err)

	countAll, err = packageRepository.CountAll()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), countAll, "expected 0 rows after DeleteAll, got %d", countAll)
}

func TestExample05_SliceNestedUDT_TupleOfUDT_insertMany(t *testing.T) {
	keyspace := "example_05"

	err := SetUpKeySpace(keyspace)
	if err != nil {
		t.Fatal(err)
	}

	_, sessionP, err := cqlxo_connection.CreateCluster(hosts, "cassandra", "", keyspace, gocql.ParseConsistency(consistencyLV), localDC, clusterTimeout, numRetries)
	if err != nil {
		t.Fatal(err)
	}
	session := *sessionP
	defer func() {
		CleanUp(session, keyspace)
		session.Close()
	}()

	packageDAO, err := mPackageRepository(session)
	if err != nil {
		t.Fatal(err)
	}

	tags := []Tag{
		{
			Label:    "electronics",
			Category: Category{Name: "tech", Code: 1},
		},
		{
			Label:    "gadget",
			Category: Category{Name: "device", Code: 2},
		},
	}

	origin := GeoPoint{
		Lat:  37.7749,
		Lng:  -122.4194,
		Info: Address{Street: "1 Market St", City: "San Francisco", Zip: 94105},
	}

	var packages []cqlxoEntity.BaseScyllaEntityInterface
	var numberOfPackages = 20
	for i := 0; i < numberOfPackages; i++ {
		pkg := &Package{
			Id:     gocql.TimeUUID(),
			Name:   fmt.Sprintf("pkg-%d", i),
			Tags:   tags,
			Origin: origin,
		}
		packages = append(packages, pkg)
	}

	err = packageDAO.SaveMany(packages)
	assert.Nil(t, err, "SaveMany failed")

	countAll, err := packageDAO.CountAll()
	assert.Nil(t, err)
	assert.Equal(t, int64(numberOfPackages), countAll, "expected %d rows, got %d", numberOfPackages, countAll)

	/////////////////////////////FIND ALL///////////////////////////////////////////
	findAll := func(packageDAO *PackageRepository) ([]*Package, error) {
		var results []*Package
		err = packageDAO.FindAll(&results)
		return results, err
	}

	pkgs, err := findAll(packageDAO)
	assert.Nil(t, err)
	assert.Equal(t, len(pkgs), len(packages))

	for i, pkg := range pkgs {
		assert.Equal(t, len(pkg.Tags), len(tags))
		for j, tag := range pkg.Tags {
			assert.Equal(t, tag.Label, tags[j].Label)
			assert.Equal(t, tag.Category.Name, tags[j].Category.Name)
			assert.Equal(t, tag.Category.Code, tags[j].Category.Code)
		}
		assert.Equal(t, pkg.Origin.Lat, origin.Lat)
		assert.Equal(t, pkg.Origin.Lng, origin.Lng)
		assert.Equal(t, pkg.Origin.Info.Street, origin.Info.Street)
		assert.Equal(t, pkg.Origin.Info.City, origin.Info.City)
		assert.Equal(t, pkg.Origin.Info.Zip, origin.Info.Zip)

		t.Logf("%d: %s", i+1, pkg.Name)
	}

	/////////////////////////////FIND ALL WITH PAGINATION///////////////////////////////////////////
	findAllWithPagination := func(packageDAO *PackageRepository, itemsPerPage int) ([]*Package, error) {
		var (
			results []*Package
			page    []byte
		)
		for i := 0; ; i++ {
			var mResults []*Package

			nextPage, err := packageDAO.FindWithOption(nil, cqlxoDAO.QueryOption{
				Page:         page,
				ItemsPerPage: itemsPerPage,
			}, &mResults)

			if err != nil {
				return nil, err
			}

			results = append(results, mResults...)

			t.Logf("Page: %d -  items: %d", i, len(mResults))
			for _, pkg := range mResults {
				log.Println(pkg.Name)
			}

			page = nextPage
			if len(nextPage) == 0 {
				break
			}
		}

		return results, nil
	}

	pkgs, err = findAllWithPagination(packageDAO, 5)
	assert.Nil(t, err)
	assert.Equal(t, len(pkgs), len(packages))

	for i, pkg := range pkgs {
		assert.Equal(t, len(pkg.Tags), len(tags))
		for j, tag := range pkg.Tags {
			assert.Equal(t, tag.Label, tags[j].Label)
			assert.Equal(t, tag.Category.Name, tags[j].Category.Name)
			assert.Equal(t, tag.Category.Code, tags[j].Category.Code)
		}
		assert.Equal(t, pkg.Origin.Lat, origin.Lat)
		assert.Equal(t, pkg.Origin.Lng, origin.Lng)
		assert.Equal(t, pkg.Origin.Info.Street, origin.Info.Street)
		assert.Equal(t, pkg.Origin.Info.City, origin.Info.City)
		assert.Equal(t, pkg.Origin.Info.Zip, origin.Info.Zip)

		t.Logf("%d: %s", i+1, pkg.Name)
	}

	///////////////////////////COUNT ALL///////////////////////////////////////////
	log.Println("Test count all")
	countPkgs, err := packageDAO.CountAll()
	assert.Nil(t, err)
	assert.Equal(t, countPkgs, int64(len(packages)))

	////////////////////////////FIND WITH INDEX////////////////////////////////////////////
	log.Println("Test find with INDEX")
	findWithIndex := func(packageDAO *PackageRepository, name string) ([]*Package, error) {
		var results []*Package
		err = packageDAO.Find(Package{
			Name: name,
		}, false, &results)
		return results, err
	}

	pkgs, err = findWithIndex(packageDAO, "pkg-10")
	assert.Nil(t, err)
	assert.Equal(t, 1, len(pkgs), "expected 1 row from Find with index, got %d", len(pkgs))
	assert.Equal(t, "pkg-10", pkgs[0].Name, "Find with index Name mismatch: got %s want %s", pkgs[0].Name, "pkg-10")
	for i, pkg := range pkgs {
		assert.Equal(t, len(pkg.Tags), len(tags))
		for j, tag := range pkg.Tags {
			assert.Equal(t, tag.Label, tags[j].Label)
			assert.Equal(t, tag.Category.Name, tags[j].Category.Name)
			assert.Equal(t, tag.Category.Code, tags[j].Category.Code)
		}
		assert.Equal(t, pkg.Origin.Lat, origin.Lat)
		assert.Equal(t, pkg.Origin.Lng, origin.Lng)
		assert.Equal(t, pkg.Origin.Info.Street, origin.Info.Street)
		assert.Equal(t, pkg.Origin.Info.City, origin.Info.City)
		assert.Equal(t, pkg.Origin.Info.Zip, origin.Info.Zip)

		t.Logf("%d: %s", i+1, pkg.Name)
	}

	////////////////////////////FIND WITH INDEX WITH PAGINATION////////////////////////////////////////////
	findWithPagination := func(packageDAO *PackageRepository, c Package, itemsPerPage int, allowFiltering bool) ([]*Package, error) {
		log.Print("Find with pagination", c)
		var (
			results []*Package
			page    []byte
		)
		for i := 0; ; i++ {
			var mResults []*Package

			nextPage, err := packageDAO.FindWithOption(c, cqlxoDAO.QueryOption{
				Page:           page,
				ItemsPerPage:   itemsPerPage,
				AllowFiltering: allowFiltering,
			}, &mResults)

			if err != nil {
				return nil, err
			}

			results = append(results, mResults...)

			//t.Logf("Page: %d -  items: %d", i, len(mResults))
			//for _, car := range mResults {
			//	log.Println(car.Model)
			//}

			page = nextPage
			if len(nextPage) == 0 {
				break
			}
		}

		return results, nil
	}

	pkgs, err = findWithPagination(packageDAO, Package{
		Name: "pkg-10",
	}, 5, false)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(pkgs), "expected 1 row from Find with index, got %d", len(pkgs))
	assert.Equal(t, "pkg-10", pkgs[0].Name, "Find with index Name mismatch: got %s want %s", pkgs[0].Name, "pkg-10")
	for i, pkg := range pkgs {
		assert.Equal(t, len(pkg.Tags), len(tags))
		for j, tag := range pkg.Tags {
			assert.Equal(t, tag.Label, tags[j].Label)
			assert.Equal(t, tag.Category.Name, tags[j].Category.Name)
			assert.Equal(t, tag.Category.Code, tags[j].Category.Code)
		}
		assert.Equal(t, pkg.Origin.Lat, origin.Lat)
		assert.Equal(t, pkg.Origin.Lng, origin.Lng)
		assert.Equal(t, pkg.Origin.Info.Street, origin.Info.Street)
		assert.Equal(t, pkg.Origin.Info.City, origin.Info.City)
		assert.Equal(t, pkg.Origin.Info.Zip, origin.Info.Zip)

		t.Logf("%d: %s", i+1, pkg.Name)
	}

}
