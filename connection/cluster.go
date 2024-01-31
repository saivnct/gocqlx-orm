package cqlxo_connection

import (
	"github.com/gocql/gocql"
	"time"
)

func CreateCluster(hosts []string, keyspace string, localDC string, clusterTimeout int, numRetries int) (*gocql.ClusterConfig, *gocql.Session, error) {
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

	if localDC != "" {
		cluster.Consistency = gocql.LocalQuorum
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.DCAwareRoundRobinPolicy(localDC))
	} else {
		cluster.Consistency = gocql.Quorum
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	}
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, nil, err
	}

	return cluster, session, nil
}
