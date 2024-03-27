package test

import (
	"fmt"
	"github.com/gocql/gocql"
	"github.com/saivnct/gocqlx-orm/connection"
	"github.com/saivnct/gocqlx-orm/dao"
	"github.com/saivnct/gocqlx-orm/entity"
	"github.com/saivnct/gocqlx-orm/utils/stringUtils"
	"log"
	"testing"
	"time"
)

func TestExample02(t *testing.T) {
	keyspace := "example_02"

	err := SetUpKeySpace(keyspace)
	if err != nil {
		t.Error(err)
		return
	}

	log.Printf("working keyspace: %s\n", keyspace)

	_, sessionP, err := cqlxo_connection.CreateCluster(hosts, keyspace, gocql.ParseConsistency(consistencyLV), localDC, clusterTimeout, numRetries)
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
		"name":          "name text",
		"colors":        "colors list<text>",
		"price_log":     "price_log car_price_log",
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
	AssertEqual(t, stringUtils.CompareSlicesOrdered(carDAO.EntityInfo.Indexes, []string{"brand", "model"}), true)

	//log.Println("Indexes", carDAO.EntityInfo.Indexes)
	//log.Println("Check UDT")

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
	//udtNames := sliceUtils.Map(udts, func(udt gocql.UDTTypeInfo) string { return udt.Name })
	//log.Printf("Car UDTs: %s\n\n", strings.Join(udtNames, ", "))

	//udtStms := sliceUtils.Map(udts, func(udt gocql.UDTTypeInfo) string { return cqlxoCodec.GetCqlCreateUDTStatement(udt) })
	//log.Printf("Car UDTs: \n%s\n\n", strings.Join(udtStms, "\n"))
	//log.Printf("Car: %s\n\n", carDAO.EntityInfo.GetGreateTableStatement())

	for _, index := range carDAO.EntityInfo.Indexes {
		var count int
		str := fmt.Sprintf("SELECT COUNT(*) FROM system_schema.indexes WHERE keyspace_name = '%s' AND table_name = '%s' AND index_name ='%s' ", keyspace, carDAO.EntityInfo.TableMetaData.Name, fmt.Sprintf("%s_%s_idx", carDAO.EntityInfo.TableMetaData.Name, index))
		//log.Println(str)
		err = session.Query(str, nil).Get(&count)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		AssertEqual(t, count, 1)
	}

	var count int
	err = session.Query(fmt.Sprintf("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s'", keyspace, Car{}.TableName()), nil).Get(&count)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, count, 1)

	var carEntities []cqlxoEntity.BaseModelInterface
	for i := 2024; i < 2050; i++ {
		car := Car{
			Id:     gocql.TimeUUID(),
			Brand:  "MyBrand",
			Model:  fmt.Sprintf("Model-%d", i),
			Year:   i,
			Name:   fmt.Sprintf("%d", i),
			Colors: []string{"red", "blue", "green"},
			PriceLog: CarPriceLog{
				Price:     100000,
				CreatedAt: time.Now(),
			},
			Reward: CarReward{
				Name:   "Best",
				Cert:   "Good",
				Reward: 120000,
			},
			Matrix: [][]int{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
			Levels: []int{1, 2, 3},
			Distributions: map[string]int{
				"VN": 100,
				"US": 200,
				"UK": 300,
			},
			MatrixMap: map[string][][]float64{
				"VN": {{1.1, 1.2, 1.3}, {1.4, 1.5, 1.6}, {1.7, 1.8, 1.9}},
				"US": {{2.1, 2.2, 2.3}, {2.4, 2.5, 2.6}, {2.7, 2.8, 2.9}},
				"UK": {{3.1, 3.2, 3.3}, {3.4, 3.5, 3.6}, {3.7, 3.8, 3.9}},
			},
			ThisIgnoreField:     "BBBBB",
			thisUnexportedField: "CCCCC",
		}

		carEntities = append(carEntities, car)
	}
	err = carDAO.SaveMany(carEntities)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	/////////////////////////////FIND ALL///////////////////////////////////////////
	findAll := func(carDAO *CarDAO) ([]Car, error) {
		var cars []Car
		err = carDAO.FindAll(&cars)
		return cars, err
	}

	cars, err := findAll(carDAO)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(cars), len(carEntities))

	/////////////////////////////FIND ALL WITH PAGINATION///////////////////////////////////////////

	/////////////////////////////FIND WITH PRIMARY KEY///////////////////////////////////////////
	findWithPrimKey := func(carDAO *CarDAO, id gocql.UUID, year int) (*Car, error) {
		var cars []Car
		err = carDAO.FindByPrimaryKey(Car{
			Id:   carEntities[0].(Car).Id,
			Year: carEntities[0].(Car).Year,
		}, &cars)

		if err != nil {
			return nil, err
		}
		if len(cars) == 0 {
			return nil, nil
		}
		return &cars[0], nil
	}

	car, err := findWithPrimKey(carDAO, carEntities[0].(Car).Id, carEntities[0].(Car).Year)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, car != nil, true)
	AssertEqual(t, car.Id, carEntities[0].(Car).Id)
	AssertEqual(t, car.Year, carEntities[0].(Car).Year)

	////////////////////////////FIND WITH INDEX////////////////////////////////////////////
	findWithIndex := func(carDAO *CarDAO, brand string) ([]Car, error) {
		var cars []Car
		err = carDAO.Find(Car{
			Brand: brand,
		}, false, &cars)
		return cars, err
	}

	cars, err = findWithIndex(carDAO, "MyBrand")
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(cars), len(carEntities))

	////////////////////////////FIND WITH INDEX WITH PAGINATION////////////////////////////////////////////
	findWithPagination := func(carDAO *CarDAO, c Car, itemsPerPage int, allowFiltering bool) ([]Car, error) {
		log.Print("Find with pagination", c)
		var (
			cars []Car
			page []byte
		)
		for i := 0; ; i++ {
			var mCars []Car

			nextPage, err := carDAO.FindWithOption(c, cqlxoDAO.QueryOption{
				Page:           page,
				ItemsPerPage:   itemsPerPage,
				AllowFiltering: allowFiltering,
			}, &mCars)

			if err != nil {
				return nil, err
			}

			cars = append(cars, mCars...)

			//t.Logf("Page: %d -  items: %d", i, len(mCars))
			//for _, car := range mCars {
			//	log.Println(car.Model)
			//}

			page = nextPage
			if len(nextPage) == 0 {
				break
			}
		}

		return cars, nil
	}

	cars, err = findWithPagination(carDAO, Car{
		Brand: "MyBrand",
	}, 5, false)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(cars), len(carEntities))

	////////////////////////////FIND WITH ALLOW FILTERING WITH PAGINATION////////////////////////////////////////////

	_, err = findWithPagination(carDAO, Car{
		Name: "2024",
	}, 5, false)
	AssertEqual(t, err != nil, true)

	cars, err = findWithPagination(carDAO, Car{
		Name: "2024",
	}, 5, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(cars), 1)

	////////////////////////////DELETE ALL////////////////////////////////////////////
	err = carDAO.DeleteAll()
	if err != nil {
		t.Errorf(err.Error())
		return
	}

}
