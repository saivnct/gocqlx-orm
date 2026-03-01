package test

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/saivnct/gocqlx-orm/connection"
	"github.com/saivnct/gocqlx-orm/dao"
	"github.com/saivnct/gocqlx-orm/entity"
	"github.com/saivnct/gocqlx-orm/utils/stringUtils"
	"github.com/scylladb/gocqlx/v3/qb"
	"github.com/stretchr/testify/assert"
)

func TestExample03(t *testing.T) {
	keyspace := "example_03"

	err := SetUpKeySpace(keyspace)
	assert.Nil(t, err)

	log.Printf("working keyspace: %s\n", keyspace)

	_, sessionP, err := cqlxo_connection.CreateCluster(hosts, keyspace, gocql.ParseConsistency(consistencyLV), localDC, clusterTimeout, numRetries)
	assert.Nil(t, err)

	session := *sessionP
	defer func() {
		CleanUp(session, keyspace)
		session.Close()
	}()

	//UDT type declare in entity Person but not implemented BaseUDTInterface => that means it already created in DB
	err = session.ExecStmt("CREATE TYPE IF NOT EXISTS working_document (name text, created_at timestamp)")
	assert.Nil(t, err)

	bookDAO, err := mBookDAO(session)
	assert.Nil(t, err)

	assetCols := map[string]string{
		"id":         "id timeuuid",
		"name":       "name text",
		"author":     "author text",
		"content":    "content text",
		"created_at": "created_at timestamp",
	}

	assert.Equal(t, len(bookDAO.EntityInfo.Columns), len(assetCols))

	for _, column := range bookDAO.EntityInfo.Columns {
		assert.Equal(t, assetCols[column.Name], column.GetCqlTypeDeclareStatement())
	}

	assert.Equal(t, bookDAO.EntityInfo.TableMetaData.Name, Book{}.TableName())
	assert.Equal(t, len(bookDAO.EntityInfo.TableMetaData.Columns), len(assetCols))
	assert.True(t, stringUtils.CompareSlicesOrdered(bookDAO.EntityInfo.TableMetaData.PartKey, []string{"author", "name"}))
	assert.True(t, stringUtils.CompareSlicesOrdered(bookDAO.EntityInfo.TableMetaData.SortKey, []string{"created_at"}))
	assert.True(t, stringUtils.CompareSlicesOrdered(bookDAO.EntityInfo.Indexes, []string{"name"}))

	//log.Printf("Book: %s\n\n", bookDAO.EntityInfo.TableMetaData)
	//log.Printf("Book: %s\n\n", bookDAO.EntityInfo.GetCreateTableStatement())

	for _, index := range bookDAO.EntityInfo.Indexes {
		var count int
		str := fmt.Sprintf("SELECT COUNT(*) FROM system_schema.indexes WHERE keyspace_name = '%s' AND table_name = '%s' AND index_name ='%s' ", keyspace, bookDAO.EntityInfo.TableMetaData.Name, fmt.Sprintf("%s_%s_idx", bookDAO.EntityInfo.TableMetaData.Name, index))
		//log.Println(str)
		err = session.Query(str, nil).Get(&count)
		assert.Nil(t, err)
		assert.Equal(t, count, 1)
	}

	var count int
	err = session.Query(fmt.Sprintf("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s'", keyspace, Book{}.TableName()), nil).Get(&count)
	assert.Nil(t, err)
	assert.Equal(t, count, 1)

	err = bookDAO.Save(Book{
		Id:        gocql.TimeUUID(),
		Name:      "book 2",
		Author:    "Kira 2",
		Content:   "my deathnote ",
		CreatedAt: time.Now(),
	})
	assert.Nil(t, err)

	var bookEntities []cqlxoEntity.BaseScyllaEntityInterface
	for i := 1; i < 10; i++ {
		book := &Book{
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
	assert.Nil(t, err)

	/////////////////////////////FIND ALL///////////////////////////////////////////
	findAll := func(bookDAO *BookDAO) ([]*Book, error) {
		var books []*Book
		err = bookDAO.FindAll(&books)
		return books, err
	}

	books, err := findAll(bookDAO)
	assert.Nil(t, err)
	assert.Equal(t, len(bookEntities)+1, len(books))

	countAll, err := bookDAO.CountAll()
	assert.Nil(t, err)
	assert.Equal(t, int64(len(bookEntities)+1), countAll)

	/////////////////////////////FIND ALL///////////////////////////////////////////
	findAll2 := func(bookDAO *BookDAO) ([]*Book, error) {
		var books []*Book
		err = bookDAO.Find(Book{}, false, &books)
		return books, err
	}

	books, err = findAll2(bookDAO)
	assert.Nil(t, err)
	assert.Equal(t, len(bookEntities)+1, len(books))

	/////////////////////////////FIND WITH PRIMARY KEY///////////////////////////////////////////
	findWithPrimKey := func(bookDAO *BookDAO, author string, name string, createdAt time.Time) (*Book, error) {
		var books []*Book
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
		return books[0], nil
	}

	var zeroTime time.Time

	randomNumber := rand.Intn(len(bookEntities))
	searchBook, ok := bookEntities[randomNumber].(*Book)
	assert.True(t, ok)

	_, err = findWithPrimKey(bookDAO, searchBook.Author, searchBook.Name, zeroTime)
	assert.True(t, errors.Is(err, cqlxoDAO.InvalidPrimaryKey))

	book, err := findWithPrimKey(bookDAO, searchBook.Author, searchBook.Name, searchBook.CreatedAt)
	assert.Nil(t, err)
	assert.NotNil(t, book)
	assert.Equal(t, book.Author, searchBook.Author)
	assert.Equal(t, book.Name, searchBook.Name)
	assert.Equal(t, book.CreatedAt.UnixMilli(), searchBook.CreatedAt.UnixMilli())

	/////////////////////////////FIND WITH PARTITION KEY///////////////////////////////////////////
	findByPartitionKey := func(bookDAO *BookDAO, author string, name string) ([]*Book, error) {
		var books []*Book
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
	assert.True(t, errors.Is(err, cqlxoDAO.InvalidPartitionKey))

	books, err = findByPartitionKey(bookDAO, "Kira", "book")
	assert.Nil(t, err)
	assert.Equal(t, len(bookEntities), len(books))
	for _, book := range books {
		assert.Equal(t, book.Author, "Kira")
		assert.Equal(t, book.Name, "book")
	}

	////////////////////////////FIND WITH ALLOW FILTERING////////////////////////////////////////////
	find := func(bookDAO *BookDAO, book Book, allowFiltering bool) ([]*Book, error) {
		var books []*Book
		err = bookDAO.Find(book, allowFiltering, &books)
		return books, err
	}

	_, err = find(bookDAO, Book{
		Author: "Kira",
	}, false)
	assert.NotNil(t, err)

	books, err = find(bookDAO, Book{
		Author: "Kira",
	}, true)
	assert.Nil(t, err)
	assert.Equal(t, len(bookEntities), len(books))

	////////////////////////////COUNT WITH ALLOW FILTERING////////////////////////////////////////////
	countQuery, err := bookDAO.Count(Book{
		Author: "Kira 2",
	}, false)
	assert.NotNil(t, err)

	countQuery, err = bookDAO.Count(Book{
		Author: "Kira 2",
	}, true)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), countQuery)

	////////////////////////////FIND WITH INDEX////////////////////////////////////////////
	books, err = find(bookDAO, Book{
		Name: "book",
	}, false)
	assert.Nil(t, err)
	assert.Equal(t, len(bookEntities), len(books))

	////////////////////////////COUNT WITH INDEX////////////////////////////////////////////
	countQuery, err = bookDAO.Count(Book{
		Name: "book 2",
	}, false)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), countQuery)

	////////////////////////////FIND WITH OK WITH PAGINATION AND SORTING WITH CK ////////////////////////////////////////////
	findWithPagination := func(bookDAO *BookDAO, b Book, itemsPerPage int, orderBy string, order qb.Order) ([]*Book, error) {
		log.Print("Find with pagination", b)
		var (
			books []*Book
			page  []byte
		)
		for i := 0; ; i++ {
			var mBooks []*Book

			nextPage, err := bookDAO.FindWithOption(b, cqlxoDAO.QueryOption{
				Page:         page,
				ItemsPerPage: itemsPerPage,
				OrderBy:      orderBy,
				Order:        order,
			}, &mBooks)

			if err != nil {
				return nil, err
			}

			books = append(books, mBooks...)

			//t.Logf("Page: %d -  items: %d", i, len(mBooks))
			//for _, book := range mBooks {
			//	log.Println(book.CreatedAt)
			//}

			page = nextPage
			if len(nextPage) == 0 {
				break
			}
		}

		return books, nil
	}

	books, err = findWithPagination(bookDAO, Book{
		Name:   "book",
		Author: "Kira",
	}, 5, "created_at", qb.DESC)
	assert.Nil(t, err)
	assert.Equal(t, len(books), len(bookEntities))

	////////////////////////////DELETE BY PRIMARY KEY////////////////////////////////////////////
	err = bookDAO.DeleteByPrimaryKey(Book{
		Name:      "book",
		Author:    "Kira",
		CreatedAt: searchBook.CreatedAt,
	})
	assert.Nil(t, err)

	book, err = findWithPrimKey(bookDAO, "Kira", "book", searchBook.CreatedAt)
	assert.Nil(t, err)
	assert.Nil(t, book)
	////////////////////////////DELETE BY PARTITION KEY////////////////////////////////////////////
	err = bookDAO.DeleteByPartitionKey(Book{
		Name:   "book",
		Author: "Kira",
	})
	assert.Nil(t, err)

	books, err = findByPartitionKey(bookDAO, "Kira", "book")
	assert.Nil(t, err)
	assert.Equal(t, len(books), 0)

	////////////////////////////DELETE ALL////////////////////////////////////////////
	err = bookDAO.SaveMany(bookEntities)
	assert.Nil(t, err)

	err = bookDAO.DeleteAll()
	assert.Nil(t, err)

	countAll, err = bookDAO.CountAll()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), countAll)
}
