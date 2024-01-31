package test

import (
	"context"
	"fmt"
	"giangbb.studio/go.cqlx.orm/connection"
	"giangbb.studio/go.cqlx.orm/utils/stringUtils"
	"github.com/davecgh/go-spew/spew"
	"github.com/gocql/gocql"
	"log"
	"strings"
	"testing"
	"time"
)

func TestExample00(t *testing.T) {
	keyspace := "example_00"

	err := SetUpKeySpace(keyspace)
	if err != nil {
		t.Error(err)
		return
	}

	log.Printf("working keyspace: %s\n", keyspace)

	_, session, err := cqlxo_connection.CreateCluster(hosts, keyspace, localDC, clusterTimeout, numRetries)
	if err != nil {
		t.Errorf("Unable to connect to cluster %v", err)
		return
	}
	defer func() {
		//CleanUp(session, keyspace)
		session.Close()
	}()

	tweetDAO, err := mTweetDAO(session)
	if err != nil {
		t.Errorf("err: %v", err)
		return
	}

	assetCols := map[string]string{
		"time_line":  "time_line text",
		"id":         "id uuid",
		"text":       "text text",
		"created_at": "created_at timestamp",
	}

	AssertEqual(t, len(tweetDAO.EntityInfo.Columns), len(assetCols))

	for _, column := range tweetDAO.EntityInfo.Columns {
		log.Println(column.String())
		//log.Printf("%s\n\n", column.GetCqlTypeDeclareStatement())
		AssertEqual(t, assetCols[column.Name], column.GetCqlTypeDeclareStatement())
	}

	AssertEqual(t, tweetDAO.EntityInfo.TableName, Tweet{}.TableName())
	AssertEqual(t, len(tweetDAO.EntityInfo.ColumnsName), len(assetCols))
	AssertEqual(t, stringUtils.CompareSlicesOrdered(tweetDAO.EntityInfo.PartKey, []string{"id"}), true)
	AssertEqual(t, stringUtils.CompareSlicesOrdered(tweetDAO.EntityInfo.SortKey, []string{"created_at"}), true)

	ctx := context.Background()

	for i := 1; i < 10; i++ {
		err = session.Query(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tweetDAO.EntityInfo.TableName, strings.Join(tweetDAO.EntityInfo.ColumnsName, ", "), stringUtils.GenerateCQLQuestionMarks(len(tweetDAO.EntityInfo.ColumnsName))),
			"me", gocql.TimeUUID(), fmt.Sprintf("hello %d", i), time.Now()).WithContext(ctx).Exec()
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
	}

	log.Printf("Tweet: %s\n\n", tweetDAO.EntityInfo.GetGreateTableStatement())

	//scanner := session.Query(fmt.Sprintf("SELECT %s FROM %s", strings.Join(tweetDAO.EntityInfo.ColumnsName, ", "), tweetDAO.EntityInfo.TableName)).WithContext(ctx).Iter().Scanner()
	//for scanner.Next() {
	//	tw := Tweet{}
	//	err = scanner.Scan(&tw.TimeLine, &tw.Id, &tw.Text, &tw.CreatedAt)
	//	if err != nil {
	//		t.Errorf("err: %v", err)
	//		return
	//	}
	//	spew.Dump(tw)
	//}

	iter := session.Query(fmt.Sprintf("SELECT %s FROM %s", strings.Join(tweetDAO.EntityInfo.ColumnsName, ", "), tweetDAO.EntityInfo.TableName)).WithContext(ctx).Iter()
	for {
		row, err := iter.RowData()
		if err != nil {
			t.Errorf("err: %v", err)
			return
		}
		spew.Dump(row)

		if !iter.Scan() {
			break
		}

	}
	iter.Close()

}
