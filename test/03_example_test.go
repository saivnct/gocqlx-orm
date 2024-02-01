package test

import (
	"errors"
	"fmt"
	"giangbb.studio/go.cqlx.orm/connection"
	cqlxoDAO "giangbb.studio/go.cqlx.orm/dao"
	cqlxoEntity "giangbb.studio/go.cqlx.orm/entity"
	"giangbb.studio/go.cqlx.orm/utils/stringUtils"
	"github.com/gocql/gocql"
	"log"
	"testing"
	"time"
)

func TestExample03(t *testing.T) {
	keyspace := "example_03"

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

	bookDAO, err := mBookDAO(session)
	if err != nil {
		t.Error(err)
		return
	}

	assetCols := map[string]string{
		"id":         "id timeuuid",
		"name":       "name text",
		"author":     "author text",
		"content":    "content text",
		"created_at": "created_at timestamp",
	}

	AssertEqual(t, len(bookDAO.EntityInfo.Columns), len(assetCols))

	for _, column := range bookDAO.EntityInfo.Columns {
		AssertEqual(t, assetCols[column.Name], column.GetCqlTypeDeclareStatement())
	}

	AssertEqual(t, bookDAO.EntityInfo.TableMetaData.Name, Book{}.TableName())
	AssertEqual(t, len(bookDAO.EntityInfo.TableMetaData.Columns), len(assetCols))
	AssertEqual(t, stringUtils.CompareSlicesOrdered(bookDAO.EntityInfo.TableMetaData.PartKey, []string{"author", "name"}), true)
	AssertEqual(t, stringUtils.CompareSlicesOrdered(bookDAO.EntityInfo.TableMetaData.SortKey, []string{"created_at"}), true)
	AssertEqual(t, stringUtils.CompareSlicesOrdered(bookDAO.EntityInfo.Indexes, []string{"name"}), true)

	log.Printf("Book: %s\n\n", bookDAO.EntityInfo.TableMetaData)
	log.Printf("Book: %s\n\n", bookDAO.EntityInfo.GetGreateTableStatement())

	for _, index := range bookDAO.EntityInfo.Indexes {
		var count int
		str := fmt.Sprintf("SELECT COUNT(*) FROM system_schema.indexes WHERE keyspace_name = '%s' AND table_name = '%s' AND index_name ='%s' ", keyspace, bookDAO.EntityInfo.TableMetaData.Name, fmt.Sprintf("%s_%s_idx", bookDAO.EntityInfo.TableMetaData.Name, index))
		//log.Println(str)
		err = session.Query(str, nil).Get(&count)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		AssertEqual(t, count, 1)
	}

	var count int
	err = session.Query(fmt.Sprintf("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s'", keyspace, Book{}.TableName()), nil).Get(&count)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, count, 1)

	err = bookDAO.Save(Book{
		Id:        gocql.TimeUUID(),
		Name:      "book 2",
		Author:    "Kira 2",
		Content:   "my deathnote ",
		CreatedAt: time.Now(),
	})
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	var books []cqlxoEntity.BaseModelInterface
	for i := 1; i < 10; i++ {
		book := Book{
			Id:        gocql.TimeUUID(),
			Name:      "book",
			Author:    "Kira",
			Content:   fmt.Sprintf("my deathnote %d", i),
			CreatedAt: time.Now(),
		}
		books = append(books, book)
		time.Sleep(10 * time.Millisecond)
	}

	err = bookDAO.SaveMany(books)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	/////////////////////////////FIND ALL///////////////////////////////////////////
	var dbBooks []Book
	err = bookDAO.FindAll(&dbBooks)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(books)+1, len(dbBooks))

	countAll, err := bookDAO.CountAll()
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, int64(len(books)+1), countAll)

	/////////////////////////////FIND WITH PRIMARY KEY///////////////////////////////////////////
	var dbBooks2 []Book
	err = bookDAO.FindByPrimaryKey(Book{
		Name:   "book",
		Author: "Kira",
	}, &dbBooks2)
	AssertEqual(t, errors.Is(err, cqlxoDAO.InvalidPrimaryKey), true)

	err = bookDAO.FindByPrimaryKey(Book{
		Name:      "book",
		Author:    "Kira",
		CreatedAt: dbBooks[len(dbBooks)-1].CreatedAt,
	}, &dbBooks2)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(dbBooks2), 1)
	if len(dbBooks2) > 0 {
		AssertEqual(t, dbBooks2[0].Author, "Kira")
		AssertEqual(t, dbBooks2[0].Name, "book")
		AssertEqual(t, dbBooks2[0].CreatedAt, dbBooks[len(dbBooks)-1].CreatedAt)
	}

	/////////////////////////////FIND WITH PARTITION KEY///////////////////////////////////////////
	var dbBooks3 []Book
	err = bookDAO.FindByPartitionKey(Book{
		Author: "Kira",
	}, &dbBooks3)
	AssertEqual(t, errors.Is(err, cqlxoDAO.InvalidPartitionKey), true)

	err = bookDAO.FindByPartitionKey(Book{
		Name:   "book",
		Author: "Kira",
	}, &dbBooks3)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(books), len(dbBooks3))
	for _, book := range dbBooks3 {
		AssertEqual(t, book.Author, "Kira")
		AssertEqual(t, book.Name, "book")
	}

	/////////////////////////////FIND ALL///////////////////////////////////////////
	var dbBooks4 []Book
	err = bookDAO.Find(Book{}, false, &dbBooks4)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(books)+1, len(dbBooks4))

	////////////////////////////FIND WITH ALLOW FILTERING////////////////////////////////////////////
	var dbBooks5 []Book
	err = bookDAO.Find(Book{
		Author: "Kira",
	}, false, &dbBooks5)
	AssertEqual(t, err != nil, true)

	err = bookDAO.Find(Book{
		Author: "Kira",
	}, true, &dbBooks5)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(books), len(dbBooks5))

	////////////////////////////COUNT WITH ALLOW FILTERING////////////////////////////////////////////
	countQuery, err := bookDAO.Count(Book{
		Author: "Kira 2",
	}, false)
	AssertEqual(t, err != nil, true)

	countQuery, err = bookDAO.Count(Book{
		Author: "Kira 2",
	}, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, int64(1), countQuery)

	////////////////////////////FIND WITH INDEX////////////////////////////////////////////
	var dbBooks6 []Book
	err = bookDAO.Find(Book{
		Name: "book",
	}, false, &dbBooks6)

	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(books), len(dbBooks6))

	////////////////////////////COUNT WITH INDEX////////////////////////////////////////////
	countQuery, err = bookDAO.Count(Book{
		Name: "book 2",
	}, false)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, int64(1), countQuery)

	////////////////////////////DELETE BY PRIMARY KEY////////////////////////////////////////////
	err = bookDAO.DeleteByPrimaryKey(Book{
		Name:      "book",
		Author:    "Kira",
		CreatedAt: dbBooks[len(dbBooks)-1].CreatedAt,
	})
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	var dbBooks7 []Book
	err = bookDAO.FindByPrimaryKey(Book{
		Name:      "book",
		Author:    "Kira",
		CreatedAt: dbBooks[len(dbBooks)-1].CreatedAt,
	}, &dbBooks7)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(dbBooks7), 0)

	////////////////////////////DELETE BY PARTITION KEY////////////////////////////////////////////
	err = bookDAO.DeleteByPartitionKey(Book{
		Name:   "book",
		Author: "Kira",
	})
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	var dbBooks8 []Book
	err = bookDAO.FindByPartitionKey(Book{
		Name:   "book",
		Author: "Kira",
	}, &dbBooks8)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(dbBooks8), 0)

	////////////////////////////DELETE ALL////////////////////////////////////////////
	err = bookDAO.SaveMany(books)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	err = bookDAO.DeleteAll()
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	countAll, err = bookDAO.CountAll()
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, int64(0), countAll)
}
