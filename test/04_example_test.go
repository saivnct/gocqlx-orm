package test

import (
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	cqlxoCodec "github.com/saivnct/gocqlx-orm/codec"
	cqlxo_connection "github.com/saivnct/gocqlx-orm/connection"
	"github.com/stretchr/testify/assert"
)

func TestExample04_NestedUDT_SliceUDT_Tuple(t *testing.T) {
	keyspace := "example_04"

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

	deliveryRepository, err := mDeliveryRepository(session)
	if err != nil {
		t.Fatal(err)
	}

	expectedCols := map[string]string{
		"id":      "id uuid",
		"name":    "name text",
		"profile": "profile frozen<delivery_profile>",
		"drop_at": "drop_at tuple<double, double>",
	}
	for _, c := range deliveryRepository.EntityInfo.Columns {
		want, ok := expectedCols[c.Name]
		assert.True(t, ok, "unexpected column %s", c.Name)

		assert.Equal(t, c.GetCqlTypeDeclareStatement(), want, "unexpected declaration for %s: got %q want %q", c.Name, c.GetCqlTypeDeclareStatement(), want)
	}

	expectedUDTStatements := map[string]string{
		"address":          "CREATE TYPE IF NOT EXISTS address (street text, city text, zip int)",
		"delivery_profile": "CREATE TYPE IF NOT EXISTS delivery_profile (primary_address frozen<address>, address_history list<frozen<address>>)",
	}
	for _, udt := range deliveryRepository.EntityInfo.ScanUDTs() {
		want, ok := expectedUDTStatements[udt.Name]
		assert.True(t, ok)

		got := cqlxoCodec.GetCqlCreateUDTStatement(udt)
		assert.Equal(t, got, want, "unexpected udt statement for %s: got %q want %q", udt.Name, got, want)
	}

	for _, udtName := range []string{"address", "delivery_profile"} {
		var count int
		err = session.Query(
			fmt.Sprintf("SELECT COUNT(*) FROM system_schema.types WHERE keyspace_name = '%s' AND type_name = '%s'", keyspace, udtName),
			nil,
		).Get(&count)
		assert.Nil(t, err)
		assert.Equal(t, count, 1, "expected udt %s to exist", udtName)
	}

	entity := &Delivery{
		Id:   gocql.TimeUUID(),
		Name: "order-01",
		Profile: DeliveryProfile{
			PrimaryAddress: Address{
				Street: "1 Main St",
				City:   "San Jose",
				Zip:    95112,
			},
			AddressHistory: []Address{
				{Street: "2 First St", City: "New York", Zip: 10001},
				{Street: "3 Lake St", City: "Austin", Zip: 73301},
			},
		},
		DropAt: Coordinate{Lat: 10.775, Lng: 106.701},
	}

	err = deliveryRepository.Save(entity)
	assert.Nil(t, err)

	countAll, err := deliveryRepository.CountAll()
	assert.Nil(t, err)
	assert.Equal(t, countAll, int64(1), "expected 1 row after insert, got %d", countAll)

	var deliveries []*Delivery
	err = deliveryRepository.FindAll(&deliveries)
	assert.Nil(t, err)
	assert.Equal(t, len(deliveries), 1, "expected 1 row from FindAll, got %d", len(deliveries))
	assert.Equal(t, deliveries[0].Id, entity.Id, "FindAll Id mismatch: got %s want %s", deliveries[0].Id, entity.Id)
	assert.Equal(t, deliveries[0].Name, entity.Name, "FindAll Name mismatch: got %s want %s", deliveries[0].Name, entity.Name)
	assert.Equal(t, deliveries[0].Profile.PrimaryAddress.Street, entity.Profile.PrimaryAddress.Street)
	assert.Equal(t, deliveries[0].Profile.PrimaryAddress.City, entity.Profile.PrimaryAddress.City)
	assert.Equal(t, deliveries[0].Profile.PrimaryAddress.Zip, entity.Profile.PrimaryAddress.Zip)
	assert.Equal(t, deliveries[0].Profile.AddressHistory[0].Street, entity.Profile.AddressHistory[0].Street)
	assert.Equal(t, deliveries[0].Profile.AddressHistory[0].City, entity.Profile.AddressHistory[0].City)
	assert.Equal(t, deliveries[0].Profile.AddressHistory[0].Zip, entity.Profile.AddressHistory[0].Zip)
	assert.Equal(t, deliveries[0].Profile.AddressHistory[1].Street, entity.Profile.AddressHistory[1].Street)
	assert.Equal(t, deliveries[0].Profile.AddressHistory[1].City, entity.Profile.AddressHistory[1].City)
	assert.Equal(t, deliveries[0].Profile.AddressHistory[1].Zip, entity.Profile.AddressHistory[1].Zip)
	assert.Equal(t, deliveries[0].DropAt.Lat, entity.DropAt.Lat, "FindAll DropAt.Lat mismatch: got %s want %s", deliveries[0].DropAt.Lat, entity.DropAt.Lat)
	assert.Equal(t, deliveries[0].DropAt.Lng, entity.DropAt.Lng, "FindAll DropAt.Lng mismatch: got %s want %s", deliveries[0].DropAt.Lng, entity.DropAt.Lng)

	var byPK []*Delivery
	err = deliveryRepository.FindByPrimaryKey(Delivery{Id: entity.Id}, &byPK)
	assert.Nil(t, err)
	assert.Equal(t, len(byPK), 1, "expected 1 row from FindByPrimaryKey, got %d", len(byPK))
	assert.Equal(t, byPK[0].Id, entity.Id, "FindByPrimaryKey Id mismatch: got %s want %s", byPK[0].Id, entity.Id)
	assert.Equal(t, byPK[0].Name, entity.Name, "FindByPrimaryKey Name mismatch: got %s want %s", byPK[0].Name, entity.Name)
	assert.Equal(t, byPK[0].Profile.PrimaryAddress.Street, entity.Profile.PrimaryAddress.Street)
	assert.Equal(t, byPK[0].Profile.PrimaryAddress.City, entity.Profile.PrimaryAddress.City)
	assert.Equal(t, byPK[0].Profile.PrimaryAddress.Zip, entity.Profile.PrimaryAddress.Zip)
	assert.Equal(t, byPK[0].Profile.AddressHistory[0].Street, entity.Profile.AddressHistory[0].Street)
	assert.Equal(t, byPK[0].Profile.AddressHistory[0].City, entity.Profile.AddressHistory[0].City)
	assert.Equal(t, byPK[0].Profile.AddressHistory[0].Zip, entity.Profile.AddressHistory[0].Zip)
	assert.Equal(t, byPK[0].Profile.AddressHistory[1].Street, entity.Profile.AddressHistory[1].Street)
	assert.Equal(t, byPK[0].Profile.AddressHistory[1].City, entity.Profile.AddressHistory[1].City)
	assert.Equal(t, byPK[0].Profile.AddressHistory[1].Zip, entity.Profile.AddressHistory[1].Zip)
	assert.Equal(t, byPK[0].DropAt.Lat, entity.DropAt.Lat, "FindAll DropAt.Lat mismatch: got %s want %s", deliveries[0].DropAt.Lat, entity.DropAt.Lat)
	assert.Equal(t, byPK[0].DropAt.Lng, entity.DropAt.Lng, "FindAll DropAt.Lng mismatch: got %s want %s", deliveries[0].DropAt.Lng, entity.DropAt.Lng)

	var (
		lat float64
		lng float64
	)
	err = session.Session.Query("SELECT drop_at FROM delivery WHERE id = ?", entity.Id).Scan(&lat, &lng)
	assert.Nil(t, err)
	assert.Equal(t, lat, entity.DropAt.Lat, "raw query DropAt.Lat mismatch: got %f want %f", lat, entity.DropAt.Lat)
	assert.Equal(t, lng, entity.DropAt.Lng, "raw query DropAt.Lng mismatch: got %f want %f", lng, entity.DropAt.Lng)

	err = deliveryRepository.DeleteAll()
	assert.Nil(t, err)

	countAll, err = deliveryRepository.CountAll()
	assert.Nil(t, err)
	assert.Equal(t, countAll, int64(0), "expected 0 rows after delete all, got %d", countAll)
}
