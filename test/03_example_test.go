package test

import (
	"giangbb.studio/go.cqlx.orm/connection"
	"giangbb.studio/go.cqlx.orm/utils/stringUtils"
	"log"
	"testing"
)

func TestExample03(t *testing.T) {
	keyspace := "example_03"

	err := SetUpKeySpace(keyspace)
	if err != nil {
		t.Error(err)
		return
	}

	log.Printf("working keyspace: %s\n", keyspace)

	_, session, err := cqlxo_connection.CreateCluster(hosts, keyspace, localDC, clusterTimeout, numRetries)
	if err != nil {
		t.Errorf("Unable to connect to cluster")
		return
	}
	defer func() {
		CleanUp(session, keyspace)
		session.Close()
	}()

	//UDT type declare in entity Person but not implemented BaseUDTInterface => that means it already created in DB
	//err = session.ExecStmt("CREATE TYPE IF NOT EXISTS working_document (name text, created_at timestamp)")
	//if err != nil {
	//	t.Errorf(err.Error())
	//	return
	//}

	bookDAO, err := mBookDAO(session)
	if err != nil {
		t.Error(err)
		return
	}

	assetCols := map[string]string{
		"id":               "id timeuuid",
		"name":             "name text",
		"author":           "author text",
		"content":          "content text",
		"working_document": "working_document working_document",
	}

	AssertEqual(t, len(bookDAO.EntityInfo.Columns), len(assetCols))

	for _, column := range bookDAO.EntityInfo.Columns {
		AssertEqual(t, assetCols[column.Name], column.GetCqlTypeDeclareStatement())
	}

	AssertEqual(t, bookDAO.EntityInfo.TableName, Book{}.TableName())
	AssertEqual(t, len(bookDAO.EntityInfo.ColumnsName), len(assetCols))
	AssertEqual(t, stringUtils.CompareSlicesOrdered(bookDAO.EntityInfo.PartKey, []string{"author", "name"}), true)
	AssertEqual(t, stringUtils.CompareSlicesOrdered(bookDAO.EntityInfo.SortKey, []string{}), true)

	log.Printf("Book: %s\n\n", bookDAO.EntityInfo)
	log.Printf("Book: %s\n\n", bookDAO.EntityInfo.GetGreateTableStatement())

	//var count int
	//err = session.Query(fmt.Sprintf("SELECT COUNT(*) FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s'", keyspace, Book{}.TableName()), nil).Get(&count)
	//if err != nil {
	//	t.Errorf(err.Error())
	//	return
	//}
	//AssertEqual(t, count, 1)
	//
	//
	//
	//

	//book := Book{
	//	Id:      gocql.TimeUUID(),
	//	Name:    "My Book",
	//	Author:  "Kira",
	//	Content: "my deathnote",
	//	WorkingDocument: WorkingDoc{
	//		Name:      "Hello World",
	//		CreatedAt: time.Now(),
	//	},
	//}
	//err = bookDAO.Save(session, book)
	//if err != nil {
	//	t.Errorf(err.Error())
	//	return
	//}
	//
	//spew.Dump(WorkingDoc{
	//	Name:      "Hello World",
	//	CreatedAt: time.Now(),
	//})

}
