package test

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/saivnct/gocqlx-orm/codec"
	"github.com/saivnct/gocqlx-orm/connection"
	"github.com/saivnct/gocqlx-orm/utils/sliceUtils"
	"github.com/saivnct/gocqlx-orm/utils/stringUtils"
	"log"
	"strings"
	"testing"
	"time"
)

func TestExample01(t *testing.T) {
	keyspace := "example_01"

	err := SetUpKeySpace(keyspace)
	if err != nil {
		t.Error(err)
		return
	}

	log.Printf("working keyspace: %s\n", keyspace)

	_, sessionP, err := cqlxo_connection.CreateCluster(hosts, keyspace, localDC, clusterTimeout, numRetries)
	if err != nil {
		t.Errorf("Unable to connect to cluster")
		return
	}
	session := *sessionP
	defer func() {
		CleanUp(session, keyspace)
		session.Close()
	}()

	//UDT type declare in entity Person but not implemented BaseUDTInterface => that means it already created in DB
	err = session.ExecStmt("CREATE TYPE IF NOT EXISTS citizen_id (id text, end_at timestamp, created_at timestamp, level int)")
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	personDAO, err := mPersonDAO(session)
	if err != nil {
		t.Error(err)
		return
	}

	assetCols := map[string]string{
		"id":              "id timeuuid",
		"last_name":       "last_name text",
		"first_name":      "first_name text",
		"favorite_place":  "favorite_place frozen<favorite_place>",
		"email":           "email text",
		"static_ip":       "static_ip inet",
		"nick_names":      "nick_names set<text>",
		"working_history": "working_history map<int, text>",
		"citizen_ident":   "citizen_ident citizen_id",
		"created_at":      "created_at timestamp",
	}

	AssertEqual(t, len(personDAO.EntityInfo.Columns), len(assetCols))

	for _, column := range personDAO.EntityInfo.Columns {
		//log.Println(column.String())
		//log.Printf("%s\n\n", column.GetCqlTypeDeclareStatement())
		AssertEqual(t, assetCols[column.Name], column.GetCqlTypeDeclareStatement())
	}

	//log.Println("Person", personDAO.EntityInfo.TableMetaData)
	AssertEqual(t, personDAO.EntityInfo.TableMetaData.Name, Person{}.TableName())
	AssertEqual(t, len(personDAO.EntityInfo.TableMetaData.Columns), len(assetCols))
	AssertEqual(t, stringUtils.CompareSlicesOrdered(personDAO.EntityInfo.TableMetaData.PartKey, []string{"first_name", "last_name"}), true)
	AssertEqual(t, stringUtils.CompareSlicesOrdered(personDAO.EntityInfo.TableMetaData.SortKey, []string{"created_at"}), true)
	AssertEqual(t, stringUtils.CompareSlicesOrdered(personDAO.EntityInfo.Indexes, []string{"last_name", "first_name", "email"}), true)

	log.Println("SortKey", personDAO.EntityInfo.TableMetaData.SortKey)
	log.Println("Check UDT")

	assetUDTs := map[string]gocql.UDTTypeInfo{
		"favorite_place": gocql.UDTTypeInfo{
			Name: "favorite_place",
			Elements: []gocql.UDTField{
				{
					Name: "city",
					Type: gocql.NewNativeType(5, gocql.TypeText, ""),
				},
				{
					Name: "country",
					Type: gocql.NewNativeType(5, gocql.TypeText, ""),
				},
				{
					Name: "population",
					Type: gocql.NewNativeType(5, gocql.TypeBigInt, ""),
				},
				{
					Name: "check_point",
					Type: gocql.NewNativeType(5, gocql.TypeList, ""),
				},
				{
					Name: "rating",
					Type: gocql.NewNativeType(5, gocql.TypeInt, ""),
				},
			},
		},
		"citizen_id": gocql.UDTTypeInfo{
			Name: "citizen_id",
			Elements: []gocql.UDTField{
				{
					Name: "id",
					Type: gocql.NewNativeType(5, gocql.TypeText, ""),
				},
				{
					Name: "end_at",
					Type: gocql.NewNativeType(5, gocql.TypeTimestamp, ""),
				},
				{
					Name: "created_at",
					Type: gocql.NewNativeType(5, gocql.TypeTimestamp, ""),
				},
				{
					Name: "level",
					Type: gocql.NewNativeType(5, gocql.TypeInt, ""),
				},
			},
		},
	}
	udts := personDAO.EntityInfo.ScanUDTs()
	//AssertEqual(t, len(udts), len(assetUDTs))
	for _, udt := range udts {
		assetUdT, ok := assetUDTs[udt.Name]
		AssertEqual(t, ok, true)
		log.Println(udt.Name)

		AssertEqual(t, assetUdT.Name, udt.Name)
		AssertEqual(t, len(assetUdT.Elements), len(udt.Elements))
		for i, element := range udt.Elements {
			AssertEqual(t, assetUdT.Elements[i].Name, element.Name)
			AssertEqual(t, assetUdT.Elements[i].Type.Type().String(), element.Type.Type().String())
		}

		var count int
		err = session.Query(fmt.Sprintf("SELECT COUNT(*) FROM system_schema.types WHERE keyspace_name = '%s' AND type_name = '%s'", keyspace, udt.Name), nil).Get(&count)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		AssertEqual(t, count, 1)
	}
	udtNames := sliceUtils.Map(udts, func(udt gocql.UDTTypeInfo) string { return udt.Name })
	log.Printf("Person UDTs: %s\n\n", strings.Join(udtNames, ", "))

	udtStms := sliceUtils.Map(udts, func(udt gocql.UDTTypeInfo) string { return cqlxoCodec.GetCqlCreateUDTStatement(udt) })
	log.Printf("Person UDTs: \n%s\n\n", strings.Join(udtStms, "\n"))
	log.Printf("Person: %s\n\n", personDAO.EntityInfo.GetGreateTableStatement())

	for _, index := range personDAO.EntityInfo.Indexes {
		var count int
		str := fmt.Sprintf("SELECT COUNT(*) FROM system_schema.indexes WHERE keyspace_name = '%s' AND table_name = '%s' AND index_name ='%s' ", keyspace, personDAO.EntityInfo.TableMetaData.Name, fmt.Sprintf("%s_%s_idx", personDAO.EntityInfo.TableMetaData.Name, index))
		//log.Println(str)
		err = session.Query(str, nil).Get(&count)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		AssertEqual(t, count, 1)
	}

	var count int
	err = session.Query(fmt.Sprintf("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s'", keyspace, Person{}.TableName()), nil).Get(&count)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, count, 1)

	person := Person{
		Id:        gocql.TimeUUID(),
		LastName:  "test",
		FirstName: "test2",
		FavoritePlace: FavoritePlace{
			City:       "HCM",
			Country:    "VN",
			Population: 0,
			CheckPoint: []string{"1", "2", "3"},
			Rating:     3,
		},
		Email:          "test@test.com",
		StaticIP:       "127.0.0.1",
		Nicknames:      []string{"test", "test2", "test3"},
		WorkingHistory: map[int]string{1: "test", 2: "test2", 3: "test3"},
		CitizenIdent: CitizenIdent{
			Id:        gocql.TimeUUID().String(),
			EndAt:     time.Time{},
			CreatedAt: time.Time{},
			Level:     10,
		},
		CreatedAt: time.Now(),
	}

	err = personDAO.Save(person)
	if err != nil {
		t.Errorf("Unable to save person -> %v", err)
		return
	}

	var persons []Person
	err = personDAO.FindAll(&persons)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	log.Println("persons", persons)

	////////////////////////////DELETE ALL////////////////////////////////////////////
	err = personDAO.DeleteAll()
	if err != nil {
		t.Errorf(err.Error())
		return
	}
}
