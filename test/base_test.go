package test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/gocql/gocql"
	"github.com/saivnct/gocqlx-orm/connection"
	"github.com/scylladb/gocqlx/v3"
	"github.com/testcontainers/testcontainers-go/modules/scylladb"
)

var (
	hosts          = []string{"localhost"}
	clusterTimeout = 10
	numRetries     = 5
	localDC        = ""
	consistencyLV  = "LOCAL_ONE"
)

func TestMain(m *testing.M) {
	InitTestEnv()
	code := m.Run()
	CloseTestEnv()
	os.Exit(code)
}

func InitTestEnv() {
	// if we crash the go code, we get the file and line number
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Print("Init test environment")

	scyllaContainerCtx := context.Background()
	scyllaContainer, err := scylladb.Run(
		context.Background(),
		"scylladb/scylla:5.2",
		scylladb.WithCustomCommands("--memory=1G", "--smp=1", "--overprovisioned=1", "--api-address=0.0.0.0"),
	)
	if err != nil {
		log.Fatalf("Failed to init scyllaContainer %v", err)
	}

	scyllaHost, err := scyllaContainer.Host(scyllaContainerCtx)
	if err != nil {
		log.Fatalf("Failed to init scyllaContainer %v", err)
	}
	log.Printf("ScyllaDB container scyllaHost: %s\n", scyllaHost)

	scyllaPort, err := scyllaContainer.MappedPort(scyllaContainerCtx, "9042/tcp")
	if err != nil {
		log.Fatalf("Failed to init scyllaContainer %v", err)
	}
	log.Printf("ScyllaDB container scyllaPort: %s\n", scyllaPort)

	hosts = []string{fmt.Sprintf("%s:%s", scyllaHost, scyllaPort.Port())}
}

func CloseTestEnv() {

}

func SetUpKeySpace(keyspace string) error {
	_, sessionP, err := cqlxo_connection.CreateCluster(hosts, "cassandra", "", "", gocql.ParseConsistency(consistencyLV), localDC, clusterTimeout, numRetries)
	if err != nil {
		return err
	}
	session := *sessionP
	defer session.Close()

	err = session.ExecStmt(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}", keyspace))

	return err
}

func CleanUp(session gocqlx.Session, keyspace string) {
	session.ExecStmt(fmt.Sprintf("DROP KEYSPACE %s", keyspace))
}
