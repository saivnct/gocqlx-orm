package test

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/gocql/gocql"
	"github.com/saivnct/gocqlx-orm/connection"
	"github.com/saivnct/gocqlx-orm/entity"
	"github.com/saivnct/gocqlx-orm/repository"
	"github.com/saivnct/gocqlx-orm/utils/stringUtils"
	"github.com/stretchr/testify/assert"
)

func TestExample02(t *testing.T) {
	keyspace := "example_02"

	err := SetUpKeySpace(keyspace)
	assert.Nil(t, err)

	log.Printf("working keyspace: %s\n", keyspace)

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

	assert.Nil(t, err)
	session := *sessionP
	defer func() {
		CleanUp(session, keyspace)
		session.Close()
	}()

	carRepository, err := mCarRepository(session)
	assert.Nil(t, err)

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

	assert.Equal(t, len(carRepository.EntityInfo.Columns), len(assetCols))

	for _, column := range carRepository.EntityInfo.Columns {
		//log.Println(column.String())
		//log.Printf("%s\n\n", column.GetCqlTypeDeclareStatement())
		assert.Equal(t, assetCols[column.Name], column.GetCqlTypeDeclareStatement())
	}

	//log.Println("Car", carRepository.EntityInfo.TableMetaData)
	assert.Equal(t, carRepository.EntityInfo.TableMetaData.Name, Car{}.TableName())
	assert.Equal(t, len(carRepository.EntityInfo.TableMetaData.Columns), len(assetCols))
	assert.True(t, stringUtils.CompareSlicesOrdered(carRepository.EntityInfo.TableMetaData.PartKey, []string{"id"}))
	assert.True(t, stringUtils.CompareSlicesOrdered(carRepository.EntityInfo.TableMetaData.SortKey, []string{"year"}))
	assert.True(t, stringUtils.CompareSlicesOrdered(carRepository.EntityInfo.Indexes, []string{"brand", "model"}))

	//log.Println("Indexes", carRepository.EntityInfo.Indexes)
	//log.Println("Check UDT")

	assetUDTs := map[string]gocql.UDTTypeInfo{
		"car_price_log": gocql.UDTTypeInfo{
			Name: "car_price_log",
			Elements: []gocql.UDTField{
				{
					Name: "price",
					Type: gocql.NewNativeType(5, gocql.TypeDouble),
				},
				{
					Name: "created_at",
					Type: gocql.NewNativeType(5, gocql.TypeTimestamp),
				},
			},
		},
		"car_reward": gocql.UDTTypeInfo{
			Name: "car_reward",
			Elements: []gocql.UDTField{
				{
					Name: "name",
					Type: gocql.NewNativeType(5, gocql.TypeText),
				},
				{
					Name: "cert",
					Type: gocql.NewNativeType(5, gocql.TypeText),
				},
				{
					Name: "reward",
					Type: gocql.NewNativeType(5, gocql.TypeDouble),
				},
			},
		},
	}
	udts := carRepository.EntityInfo.ScanUDTs()
	assert.Equal(t, len(udts), len(assetUDTs))
	for _, udt := range udts {
		assetUdT, ok := assetUDTs[udt.Name]
		assert.True(t, ok)
		log.Println(udt.Name)

		assert.Equal(t, assetUdT.Name, udt.Name)
		assert.Equal(t, len(assetUdT.Elements), len(udt.Elements))
		for i, element := range udt.Elements {
			assert.Equal(t, assetUdT.Elements[i].Name, element.Name)
			assert.Equal(t, assetUdT.Elements[i].Type.Type().String(), element.Type.Type().String())
		}

		var count int
		err = session.Query(fmt.Sprintf("SELECT COUNT(*) FROM system_schema.types WHERE keyspace_name = '%s' AND type_name = '%s'", keyspace, udt.Name), nil).Get(&count)
		assert.Nil(t, err)
		assert.Equal(t, count, 1)
	}
	//udtNames := sliceUtils.Map(udts, func(udt gocql.UDTTypeInfo) string { return udt.Name })
	//log.Printf("Car UDTs: %s\n\n", strings.Join(udtNames, ", "))

	//udtStms := sliceUtils.Map(udts, func(udt gocql.UDTTypeInfo) string { return cqlxoCodec.GetCqlCreateUDTStatement(udt) })
	//log.Printf("Car UDTs: \n%s\n\n", strings.Join(udtStms, "\n"))
	//log.Printf("Car: %s\n\n", carRepository.EntityInfo.GetCreateTableStatement())

	for _, index := range carRepository.EntityInfo.Indexes {
		var count int
		str := fmt.Sprintf("SELECT COUNT(*) FROM system_schema.indexes WHERE keyspace_name = '%s' AND table_name = '%s' AND index_name ='%s' ", keyspace, carRepository.EntityInfo.TableMetaData.Name, fmt.Sprintf("%s_%s_idx", carRepository.EntityInfo.TableMetaData.Name, index))
		//log.Println(str)
		err = session.Query(str, nil).Get(&count)
		assert.Nil(t, err)
		assert.Equal(t, count, 1)
	}

	var count int
	err = session.Query(fmt.Sprintf("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s'", keyspace, Car{}.TableName()), nil).Get(&count)
	assert.Nil(t, err)
	assert.Equal(t, count, 1)

	var carEntities []cqlxoEntity.BaseScyllaEntityInterface
	for i := 2024; i < 2060; i++ {
		car := &Car{
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
	err = carRepository.SaveMany(carEntities)
	assert.Nil(t, err)

	/////////////////////////////FIND ALL///////////////////////////////////////////
	findAll := func(carDAO *CarRepository) ([]*Car, error) {
		var results []*Car
		err = carDAO.FindAll(&results)
		return results, err
	}

	cars, err := findAll(carRepository)
	assert.Nil(t, err)
	assert.Equal(t, len(cars), len(carEntities))

	for _, car := range cars {
		assert.Equal(t, car.Brand, "MyBrand")
		assert.Equal(t, car.Colors, []string{"red", "blue", "green"})
		assert.Equal(t, car.PriceLog.Price, 100000.0)
		assert.Equal(t, car.Reward.Name, "Best")
		assert.Equal(t, car.Reward.Cert, "Good")
		assert.Equal(t, car.Reward.Reward, 120000.0)
		assert.Equal(t, car.Matrix, [][]int{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}})
		assert.Equal(t, car.Levels, []int{1, 2, 3})
		assert.Equal(t, car.Distributions, map[string]int{
			"VN": 100,
			"US": 200,
			"UK": 300,
		})
		assert.Equal(t, car.MatrixMap, map[string][][]float64{
			"VN": {{1.1, 1.2, 1.3}, {1.4, 1.5, 1.6}, {1.7, 1.8, 1.9}},
			"US": {{2.1, 2.2, 2.3}, {2.4, 2.5, 2.6}, {2.7, 2.8, 2.9}},
			"UK": {{3.1, 3.2, 3.3}, {3.4, 3.5, 3.6}, {3.7, 3.8, 3.9}},
		})

	}

	/////////////////////////////FIND ALL WITH PAGINATION///////////////////////////////////////////
	findAllWithPagination := func(carDAO *CarRepository, itemsPerPage int) ([]*Car, error) {
		var (
			cars []*Car
			page []byte
		)
		for i := 0; ; i++ {
			var mCars []*Car

			nextPage, err := carDAO.FindWithOption(nil, cqlxoRepository.QueryOption{
				Page:         page,
				ItemsPerPage: itemsPerPage,
			}, &mCars)

			if err != nil {
				return nil, err
			}

			cars = append(cars, mCars...)

			t.Logf("Page: %d -  items: %d", i, len(mCars))
			for _, car := range mCars {
				log.Println(car.Model)
			}

			page = nextPage
			if len(nextPage) == 0 {
				break
			}
		}

		return cars, nil
	}

	cars, err = findAllWithPagination(carRepository, 5)
	assert.Nil(t, err)
	assert.Equal(t, len(cars), len(carEntities))

	///////////////////////////COUNT ALL///////////////////////////////////////////
	log.Println("Test count all")
	countCars, err := carRepository.CountAll()
	assert.Nil(t, err)
	assert.Equal(t, countCars, int64(len(carEntities)))

	/////////////////////////////FIND WITH PRIMARY KEY///////////////////////////////////////////
	log.Println("Test find with primary key")
	findWithPrimKey := func(carDAO *CarRepository, id gocql.UUID, year int) (*Car, error) {
		var results []*Car
		err = carDAO.FindByPrimaryKey(Car{
			Id:   id,
			Year: year,
		}, &results)

		if err != nil {
			return nil, err
		}
		if len(results) == 0 {
			return nil, nil
		}
		return results[0], nil
	}

	randomNumber := rand.Intn(len(carEntities))
	searchCar, ok := carEntities[randomNumber].(*Car)
	assert.True(t, ok)

	car, err := findWithPrimKey(carRepository, searchCar.Id, searchCar.Year)
	spew.Dump(car)
	assert.Nil(t, err)
	assert.NotNil(t, car)
	assert.Equal(t, car.Id, searchCar.Id)
	assert.Equal(t, car.Year, searchCar.Year)
	assert.Equal(t, car.Brand, "MyBrand")
	assert.Equal(t, car.Colors, []string{"red", "blue", "green"})
	assert.Equal(t, car.PriceLog.Price, 100000.0)
	assert.Equal(t, car.Reward.Name, "Best")
	assert.Equal(t, car.Reward.Cert, "Good")
	assert.Equal(t, car.Reward.Reward, 120000.0)
	assert.Equal(t, car.Matrix, [][]int{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}})
	assert.Equal(t, car.Levels, []int{1, 2, 3})
	assert.Equal(t, car.Distributions, map[string]int{
		"VN": 100,
		"US": 200,
		"UK": 300,
	})
	assert.Equal(t, car.MatrixMap, map[string][][]float64{
		"VN": {{1.1, 1.2, 1.3}, {1.4, 1.5, 1.6}, {1.7, 1.8, 1.9}},
		"US": {{2.1, 2.2, 2.3}, {2.4, 2.5, 2.6}, {2.7, 2.8, 2.9}},
		"UK": {{3.1, 3.2, 3.3}, {3.4, 3.5, 3.6}, {3.7, 3.8, 3.9}},
	})

	////////////////////////////FIND WITH INDEX////////////////////////////////////////////
	log.Println("Test find with INDEX")
	findWithIndex := func(carDAO *CarRepository, brand string) ([]*Car, error) {
		var results []*Car
		err = carDAO.Find(Car{
			Brand: brand,
		}, false, &results)
		return results, err
	}

	cars, err = findWithIndex(carRepository, "MyBrand")
	assert.Nil(t, err)
	assert.Equal(t, len(cars), len(carEntities))
	for _, car := range cars {
		assert.Equal(t, car.Brand, "MyBrand")
	}

	////////////////////////////FIND WITH INDEX WITH PAGINATION////////////////////////////////////////////
	findWithPagination := func(carDAO *CarRepository, c Car, itemsPerPage int, allowFiltering bool) ([]*Car, error) {
		log.Print("Find with pagination", c)
		var (
			cars []*Car
			page []byte
		)
		for i := 0; ; i++ {
			var mCars []*Car

			nextPage, err := carDAO.FindWithOption(c, cqlxoRepository.QueryOption{
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

	cars, err = findWithPagination(carRepository, Car{
		Brand: "MyBrand",
	}, 5, false)
	assert.Nil(t, err)
	assert.Equal(t, len(cars), len(carEntities))
	for _, car := range cars {
		assert.Equal(t, car.Brand, "MyBrand")
	}

	////////////////////////////FIND WITH ALLOW FILTERING WITH PAGINATION////////////////////////////////////////////

	_, err = findWithPagination(carRepository, Car{
		Name: "2024",
	}, 5, false)
	assert.NotNil(t, err)

	cars, err = findWithPagination(carRepository, Car{
		Name: "2024",
	}, 5, true)
	assert.Nil(t, err)
	assert.Equal(t, len(cars), 1)
	for _, car := range cars {
		assert.Equal(t, car.Name, "2024")
	}

	////////////////////////////DELETE ALL////////////////////////////////////////////
	err = carRepository.DeleteAll()
	assert.Nil(t, err)

	countCars, err = carRepository.CountAll()
	assert.Nil(t, err)
	assert.Equal(t, countCars, int64(0))

}
