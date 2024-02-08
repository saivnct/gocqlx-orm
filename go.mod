module github.com/saivnct/gocqlx-orm

go 1.21.1

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.12.0

require (
	github.com/davecgh/go-spew v1.1.1
	github.com/gocql/gocql v1.6.0
	github.com/scylladb/go-reflectx v1.0.1
	github.com/scylladb/gocqlx/v2 v2.8.0
	gopkg.in/inf.v0 v0.9.1
)

require (
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/kr/text v0.2.0 // indirect
)
