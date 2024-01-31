package test

import (
	"fmt"
	cqlxo_connection "giangbb.studio/go.cqlx.orm/connection"
	"github.com/gocql/gocql"
	"os"
	"testing"
)

var (
	hosts          = []string{"192.168.31.41"}
	clusterTimeout = 10
	numRetries     = 5
	localDC        = ""
)

func TestMain(m *testing.M) {
	InitTestEnv()
	code := m.Run()
	CloseTestEnv()
	os.Exit(code)
}

func InitTestEnv() {

}

func CloseTestEnv() {

}

func SetUpKeySpace(keyspace string) error {
	_, session, err := cqlxo_connection.CreateCluster(hosts, "", localDC, clusterTimeout, numRetries)
	if err != nil {
		return err
	}
	defer session.Close()

	err = session.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}", keyspace)).Exec()

	return err
}

func CleanUp(session *gocql.Session, keyspace string) {
	session.Query(fmt.Sprintf("DROP KEYSPACE %s", keyspace)).Exec()
}

func AssertEqual(t *testing.T, x, y interface{}) {
	if x != y {
		t.Error("Expected ", y, ", got ", x)
	}
}
