package test

import (
	"fmt"
	cqlxoCodec "giangbb.studio/go.cqlx.orm/codec"
	"giangbb.studio/go.cqlx.orm/connection"
	"giangbb.studio/go.cqlx.orm/utils/sliceUtils"
	"giangbb.studio/go.cqlx.orm/utils/stringUtils"
	"github.com/gocql/gocql"
	"log"
	"strings"
	"testing"
)

func TestExample02(t *testing.T) {
	keyspace := "example_02"

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

	carDAO, err := mCarDAO(session)
	if err != nil {
		t.Error(err)
		return
	}

	assetCols := map[string]string{
		"id":            "id uuid",
		"brand":         "brand text",
		"model":         "model text",
		"year":          "year int",
		"colors":        "colors list<text>",
		"price_logs":    "price_logs list<frozen<car_price_log>>",
		"reward":        "reward car_reward",
		"matrix":        "matrix list<frozen<list<int>>>",
		"levels":        "levels list<int>",
		"distributions": "distributions map<text, int>",
		"matrix_map":    "matrix_map map<text, frozen<list<frozen<list<double>>>>>",
	}

	AssertEqual(t, len(carDAO.EntityInfo.Columns), len(assetCols))

	for _, column := range carDAO.EntityInfo.Columns {
		//log.Println(column.String())
		//log.Printf("%s\n\n", column.GetCqlTypeDeclareStatement())
		AssertEqual(t, assetCols[column.Name], column.GetCqlTypeDeclareStatement())
	}

	//log.Println("Car", carDAO.EntityInfo.TableMetaData)
	AssertEqual(t, carDAO.EntityInfo.TableMetaData.Name, Car{}.TableName())
	AssertEqual(t, len(carDAO.EntityInfo.TableMetaData.Columns), len(assetCols))
	AssertEqual(t, stringUtils.CompareSlicesOrdered(carDAO.EntityInfo.TableMetaData.PartKey, []string{"id"}), true)
	AssertEqual(t, stringUtils.CompareSlicesOrdered(carDAO.EntityInfo.TableMetaData.SortKey, []string{"year"}), true)

	log.Println("SortKey", carDAO.EntityInfo.TableMetaData.SortKey)
	log.Println("Check UDT")

	assetUDTs := map[string]gocql.UDTTypeInfo{
		"car_price_log": gocql.UDTTypeInfo{
			Name: "car_price_log",
			Elements: []gocql.UDTField{
				{
					Name: "price",
					Type: gocql.NewNativeType(5, gocql.TypeDouble, ""),
				},
				{
					Name: "created_at",
					Type: gocql.NewNativeType(5, gocql.TypeTimestamp, ""),
				},
			},
		},
		"car_reward": gocql.UDTTypeInfo{
			Name: "car_reward",
			Elements: []gocql.UDTField{
				{
					Name: "name",
					Type: gocql.NewNativeType(5, gocql.TypeText, ""),
				},
				{
					Name: "cert",
					Type: gocql.NewNativeType(5, gocql.TypeText, ""),
				},
				{
					Name: "reward",
					Type: gocql.NewNativeType(5, gocql.TypeDouble, ""),
				},
			},
		},
	}
	udts := carDAO.EntityInfo.ScanUDTs()
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
	udtNames := sliceUtils.Map(udts, func(udt gocql.UDTTypeInfo) string { return udt.Name })
	log.Printf("Car UDTs: %s\n\n", strings.Join(udtNames, ", "))

	udtStms := sliceUtils.Map(udts, func(udt gocql.UDTTypeInfo) string { return cqlxoCodec.GetCqlCreateUDTStatement(udt) })
	log.Printf("Car UDTs: \n%s\n\n", strings.Join(udtStms, "\n"))
	log.Printf("Car: %s\n\n", carDAO.EntityInfo.GetGreateTableStatement())

	var count int
	err = session.Query(fmt.Sprintf("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s'", keyspace, Car{}.TableName()), nil).Get(&count)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, count, 1)
}
