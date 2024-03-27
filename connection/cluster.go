package cqlxo_connection

import (
	"github.com/scylladb/gocqlx/v2"
	"time"

	"github.com/gocql/gocql"
)

func CreateCluster(hosts []string, keyspace string, consistencyLevel gocql.Consistency, localDC string, clusterTimeout int, numRetries int) (*gocql.ClusterConfig, *gocqlx.Session, error) {
	retryPolicy := &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        10 * time.Second,
		NumRetries: numRetries,
	}
	cluster := gocql.NewCluster(hosts...)

	if len(keyspace) > 0 {
		cluster.Keyspace = keyspace
	}

	cluster.Timeout = time.Duration(clusterTimeout) * time.Second
	cluster.RetryPolicy = retryPolicy
	cluster.Consistency = consistencyLevel

	if localDC != "" {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(localDC))
	} else {
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	}

	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		return nil, nil, err
	}

	return cluster, &session, nil
}
