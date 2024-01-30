package test

import (
	"fmt"
	cqlxo_connection "giangbb.studio/go.cqlx.orm/connection"
	"github.com/scylladb/gocqlx/v2"
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
	_, sessionP, err := cqlxo_connection.CreateCluster(hosts, "", localDC, clusterTimeout, numRetries)
	if err != nil {
		return err
	}
	session := *sessionP
	defer session.Close()

	err = session.ExecStmt(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor' : 1}", keyspace))

	return err
}

func CleanUp(session gocqlx.Session, keyspace string) {
	session.ExecStmt(fmt.Sprintf("DROP KEYSPACE %s", keyspace))
}

func AssertEqual(t *testing.T, x, y interface{}) {
	if x != y {
		t.Error("Expected ", y, ", got ", x)
	}
}
