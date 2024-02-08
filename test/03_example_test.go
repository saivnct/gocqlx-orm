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

	//log.Printf("Book: %s\n\n", bookDAO.EntityInfo.TableMetaData)
	//log.Printf("Book: %s\n\n", bookDAO.EntityInfo.GetGreateTableStatement())

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

	var bookEntities []cqlxoEntity.BaseModelInterface
	for i := 1; i < 10; i++ {
		book := Book{
			Id:        gocql.TimeUUID(),
			Name:      "book",
			Author:    "Kira",
			Content:   fmt.Sprintf("my deathnote %d", i),
			CreatedAt: time.Now(),
		}
		bookEntities = append(bookEntities, book)
		time.Sleep(10 * time.Millisecond)
	}

	err = bookDAO.SaveMany(bookEntities)
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	/////////////////////////////FIND ALL///////////////////////////////////////////
	findAll := func(bookDAO *BookDAO) ([]Book, error) {
		var books []Book
		err = bookDAO.FindAll(&books)
		return books, err
	}

	books, err := findAll(bookDAO)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(bookEntities)+1, len(books))

	countAll, err := bookDAO.CountAll()
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, int64(len(bookEntities)+1), countAll)

	/////////////////////////////FIND ALL///////////////////////////////////////////
	findAll2 := func(bookDAO *BookDAO) ([]Book, error) {
		var books []Book
		err = bookDAO.Find(Book{}, false, &books)
		return books, err
	}

	books, err = findAll2(bookDAO)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(bookEntities)+1, len(books))

	/////////////////////////////FIND WITH PRIMARY KEY///////////////////////////////////////////
	findWithPrimKey := func(bookDAO *BookDAO, author string, name string, createdAt time.Time) (*Book, error) {
		var books []Book
		b := Book{
			Author: author,
			Name:   name,
		}

		if !createdAt.IsZero() {
			b.CreatedAt = createdAt
		}

		err = bookDAO.FindByPrimaryKey(b, &books)

		if err != nil {
			return nil, err
		}
		if len(books) == 0 {
			return nil, nil
		}
		return &books[0], nil
	}

	var zeroTime time.Time
	_, err = findWithPrimKey(bookDAO, bookEntities[len(bookEntities)-1].(Book).Author, bookEntities[len(bookEntities)-1].(Book).Name, zeroTime)
	AssertEqual(t, errors.Is(err, cqlxoDAO.InvalidPrimaryKey), true)

	book, err := findWithPrimKey(bookDAO, bookEntities[len(bookEntities)-1].(Book).Author, bookEntities[len(bookEntities)-1].(Book).Name, bookEntities[len(bookEntities)-1].(Book).CreatedAt)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, book != nil, true)
	AssertEqual(t, book.Author, bookEntities[len(bookEntities)-1].(Book).Author)
	AssertEqual(t, book.Name, bookEntities[len(bookEntities)-1].(Book).Name)
	AssertEqual(t, book.CreatedAt.UnixMilli(), bookEntities[len(bookEntities)-1].(Book).CreatedAt.UnixMilli())

	/////////////////////////////FIND WITH PARTITION KEY///////////////////////////////////////////
	findByPartitionKey := func(bookDAO *BookDAO, author string, name string) ([]Book, error) {
		var books []Book
		b := Book{}
		if author != "" {
			b.Author = author
		}
		if name != "" {
			b.Name = name
		}

		err = bookDAO.FindByPartitionKey(b, &books)
		return books, err
	}

	_, err = findByPartitionKey(bookDAO, "Kira", "")
	AssertEqual(t, errors.Is(err, cqlxoDAO.InvalidPartitionKey), true)

	books, err = findByPartitionKey(bookDAO, "Kira", "book")
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(bookEntities), len(books))
	for _, book := range books {
		AssertEqual(t, book.Author, "Kira")
		AssertEqual(t, book.Name, "book")
	}

	////////////////////////////FIND WITH ALLOW FILTERING////////////////////////////////////////////
	find := func(bookDAO *BookDAO, book Book, allowFiltering bool) ([]Book, error) {
		var books []Book
		err = bookDAO.Find(book, allowFiltering, &books)
		return books, err
	}

	_, err = find(bookDAO, Book{
		Author: "Kira",
	}, false)
	AssertEqual(t, err != nil, true)

	books, err = find(bookDAO, Book{
		Author: "Kira",
	}, true)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(bookEntities), len(books))

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
	books, err = find(bookDAO, Book{
		Name: "book",
	}, false)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(bookEntities), len(books))

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
		CreatedAt: bookEntities[len(bookEntities)-1].(Book).CreatedAt,
	})
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	book, err = findWithPrimKey(bookDAO, "Kira", "book", bookEntities[len(bookEntities)-1].(Book).CreatedAt)
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, book == nil, true)
	////////////////////////////DELETE BY PARTITION KEY////////////////////////////////////////////
	err = bookDAO.DeleteByPartitionKey(Book{
		Name:   "book",
		Author: "Kira",
	})
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	books, err = findByPartitionKey(bookDAO, "Kira", "book")
	if err != nil {
		t.Errorf(err.Error())
		return
	}
	AssertEqual(t, len(books), 0)

	////////////////////////////DELETE ALL////////////////////////////////////////////
	err = bookDAO.SaveMany(bookEntities)
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
