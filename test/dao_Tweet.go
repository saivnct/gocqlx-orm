package test

import (
	cqlxoDAO "giangbb.studio/go.cqlx.orm/dao"
	"github.com/gocql/gocql"
)

type TweetDAO struct {
	cqlxoDAO.DAO
}

func mTweetDAO(session *gocql.Session) (*TweetDAO, error) {
	d := &TweetDAO{}
	err := d.InitDAO(session, Tweet{})

	return d, err
}
