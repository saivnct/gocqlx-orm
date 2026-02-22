package test

import (
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	cqlxoCodec "github.com/saivnct/gocqlx-orm/codec"
	cqlxo_connection "github.com/saivnct/gocqlx-orm/connection"
)

func TestExample04_NestedUDT_SliceUDT_Tuple(t *testing.T) {
	keyspace := "example_04"

	err := SetUpKeySpace(keyspace)
	if err != nil {
		t.Fatal(err)
	}

	_, sessionP, err := cqlxo_connection.CreateCluster(hosts, keyspace, gocql.ParseConsistency(consistencyLV), localDC, clusterTimeout, numRetries)
	if err != nil {
		t.Fatal(err)
	}
	session := *sessionP
	defer func() {
		CleanUp(session, keyspace)
		session.Close()
	}()

	deliveryDAO, err := mDeliveryDAO(session)
	if err != nil {
		t.Fatal(err)
	}

	expectedCols := map[string]string{
		"id":      "id uuid",
		"name":    "name text",
		"profile": "profile frozen<delivery_profile>",
		"drop_at": "drop_at tuple<double, double>",
	}
	for _, c := range deliveryDAO.EntityInfo.Columns {
		want, ok := expectedCols[c.Name]
		if !ok {
			t.Fatalf("unexpected column %s", c.Name)
		}
		if c.GetCqlTypeDeclareStatement() != want {
			t.Fatalf("unexpected declaration for %s: got %q want %q", c.Name, c.GetCqlTypeDeclareStatement(), want)
		}
	}

	expectedUDTStatements := map[string]string{
		"address":          "CREATE TYPE IF NOT EXISTS address (street text, city text, zip int)",
		"delivery_profile": "CREATE TYPE IF NOT EXISTS delivery_profile (primary_address frozen<address>, address_history list<frozen<address>>)",
	}
	for _, udt := range deliveryDAO.EntityInfo.ScanUDTs() {
		want, ok := expectedUDTStatements[udt.Name]
		if !ok {
			continue
		}
		got := cqlxoCodec.GetCqlCreateUDTStatement(udt)
		if got != want {
			t.Fatalf("unexpected udt statement for %s: got %q want %q", udt.Name, got, want)
		}
	}

	for _, udtName := range []string{"address", "delivery_profile"} {
		var count int
		err = session.Query(
			fmt.Sprintf("SELECT COUNT(*) FROM system_schema.types WHERE keyspace_name = '%s' AND type_name = '%s'", keyspace, udtName),
			nil,
		).Get(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("expected udt %s to exist", udtName)
		}
	}

	entity := Delivery{
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

	if err = deliveryDAO.Save(entity); err != nil {
		t.Fatal(err)
	}

	countAll, err := deliveryDAO.CountAll()
	if err != nil {
		t.Fatal(err)
	}
	if countAll != 1 {
		t.Fatalf("expected 1 row after insert, got %d", countAll)
	}

	var deliveries []Delivery
	if err = deliveryDAO.FindAll(&deliveries); err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 1 {
		t.Fatalf("expected 1 row from FindAll, got %d", len(deliveries))
	}
	if deliveries[0].Name != entity.Name {
		t.Fatalf("FindAll mismatch: got %q want %q", deliveries[0].Name, entity.Name)
	}

	var byPK []Delivery
	if err = deliveryDAO.FindByPrimaryKey(Delivery{Id: entity.Id}, &byPK); err != nil {
		t.Fatal(err)
	}
	if len(byPK) != 1 {
		t.Fatalf("expected 1 row from FindByPrimaryKey, got %d", len(byPK))
	}
	if byPK[0].DropAt.Lat != entity.DropAt.Lat || byPK[0].DropAt.Lng != entity.DropAt.Lng {
		t.Fatalf("FindByPrimaryKey tuple mismatch: got (%f,%f) want (%f,%f)", byPK[0].DropAt.Lat, byPK[0].DropAt.Lng, entity.DropAt.Lat, entity.DropAt.Lng)
	}

	var (
		lat float64
		lng float64
	)
	if err = session.Session.Query("SELECT drop_at FROM delivery WHERE id = ?", entity.Id).Scan(&lat, &lng); err != nil {
		t.Fatal(err)
	}
	if lat != entity.DropAt.Lat || lng != entity.DropAt.Lng {
		t.Fatalf("tuple values mismatch: got (%f,%f) want (%f,%f)", lat, lng, entity.DropAt.Lat, entity.DropAt.Lng)
	}

	if err = deliveryDAO.DeleteAll(); err != nil {
		t.Fatal(err)
	}
	countAll, err = deliveryDAO.CountAll()
	if err != nil {
		t.Fatal(err)
	}
	if countAll != 0 {
		t.Fatalf("expected 0 rows after delete all, got %d", countAll)
	}
}
