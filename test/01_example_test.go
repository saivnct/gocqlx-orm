package test

import (
	"fmt"
	"giangbb.studio/go.cqlx.orm/connection"
	"giangbb.studio/go.cqlx.orm/utils/stringUtils"
	"github.com/gocql/gocql"
	"log"
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
		//CleanUp(session, keyspace)
		session.Close()
	}()

	//UDT type declare in entity Person but not implemented BaseUDTInterface => that means it already created in DB
	err = session.ExecStmt("CREATE TYPE IF NOT EXISTS working_document (name text, created_at timestamp)")
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
		"id":                "id timeuuid",
		"last_name":         "last_name text",
		"first_name":        "first_name text",
		"favorite_place":    "favorite_place frozen<favorite_place>",
		"email":             "email text",
		"static_ip":         "static_ip inet",
		"nick_names":        "nick_names set<text>",
		"working_history":   "working_history map<int, text>",
		"working_documents": "working_documents list<frozen<working_document>>",
		"citizen_ident":     "citizen_ident tuple<text, timestamp, timestamp, int>",
		"created_at":        "created_at timestamp",
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

	log.Println("SortKey", personDAO.EntityInfo.TableMetaData.SortKey)
	log.Println("Check UDT")

	assetUDTs := map[string]gocql.UDTTypeInfo{
		"favorite_place": gocql.UDTTypeInfo{
			Name: "favorite_place",
			Elements: []gocql.UDTField{
				{
					Name: "land_mark",
					Type: gocql.NewNativeType(5, gocql.TypeUDT, ""),
				},
				{
					Name: "rating",
					Type: gocql.NewNativeType(5, gocql.TypeInt, ""),
				},
			},
		},
		"land_mark": gocql.UDTTypeInfo{
			Name: "land_mark",
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
					Type: gocql.CollectionType{
						NativeType: gocql.NewNativeType(5, gocql.TypeList, ""),
						Elem:       gocql.NewNativeType(5, gocql.TypeText, ""),
					},
				},
			},
		},
	}
	udts := personDAO.EntityInfo.ScanUDTs()
	AssertEqual(t, len(udts), len(assetUDTs))
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
	//udtNames := sliceUtils.Map(udts, func(udt gocql.UDTTypeInfo) string { return udt.Name })
	//log.Printf("Person UDTs: %s\n\n", strings.Join(udtNames, ", "))
	//
	//udtStms := sliceUtils.Map(udts, func(udt gocql.UDTTypeInfo) string { return cqlxoCodec.GetCqlCreateUDTStatement(udt) })
	//log.Printf("Person UDTs: \n%s\n\n", strings.Join(udtStms, "\n"))
	//log.Printf("Person: %s\n\n", personDAO.EntityInfo.GetGreateTableStatement())

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
			Place: LandMark{
				City:       "HCM",
				Country:    "VN",
				Population: 0,
				CheckPoint: []string{"1", "2", "3"},
			},
			Rating: 5,
		},
		Email:          "test@test.com",
		StaticIP:       "127.0.0.1",
		Nicknames:      []string{"test", "test2", "test3"},
		WorkingHistory: nil,
		WorkingDocuments: []WorkingDoc{
			{
				Name:      "WorkingDoc1",
				CreatedAt: time.Now(),
			},
		},
		CitizenIdent: CitizenIdent{
			Id:        gocql.TimeUUID().String(),
			EndAt:     time.Now(),
			CreatedAt: time.Now(),
			Level:     8,
		},
		CreatedAt: time.Now(),
	}

	q := session.Query(personDAO.EntityInfo.Table.Insert()).BindStruct(person)
	err = q.ExecRelease()

	//err = personDAO.Insert(session, person)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	//var persons []Person
	//err = personDAO.FindAll(session, &persons)
	//if err != nil {
	//	t.Errorf(err.Error())
	//	return
	//}
	//
	//log.Println("persons", persons)
}
