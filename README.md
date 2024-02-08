# [Giangbb Studio] - GoCqlxORM driver

GoCqlxORM driver makes working with Scylla in go easy and less error-prone.
It’s developed based on [Gocqlx](https://github.com/scylladb/gocqlx) with supporting ORM feature

## ORM Features

* Auto create table if not exists
* Auto create index if not exists
* Auto create UDT if not exists
* DAO (Data Access Object) for CRUD operations


## Installation

```bash
    go get -u github.com/saivnct/gocqlx-orm.git
```

## Getting started

Wrap gocql Session:

```go
// Create gocql cluster.
cluster := gocql.NewCluster(hosts...)
// Wrap session on creation, gocqlx session embeds gocql.Session pointer. 
session, err := gocqlx.WrapSession(cluster.CreateSession())
if err != nil {
	t.Fatal(err)
}
```

define entity:
```go
type Person struct {
    ID         gocql.UUID `db:"id" pk:"1"`
    FirstName  string     `db:"first_name" index:"true"`
    LastName   string     `db:"last_name"`
    Email      string     `db:"email"`
	StaticIP   string     `db:"static_ip" dbType:"inet"`
    Age        int        `db:"age"`
    CreatedAt  time.Time  `ck:"1"`
}

func (p Person) TableName() string {
    return "person"
}
```

**Note**: Entities's struct must implement BaseModelInterface.

Tags used in the struct:
* `db` - column name in the database (optional - if not present field name with CamelToSnakeASCII is used)
* `pk` - primary key, the value is the order of the primary key
* `ck` - clustering key, the value is the order of the clustering key (optional)
* `index` - create index for the column (optional)
* `dbType` - define the type of the column in the database (optional - if not present the type is inferred from the field type)



Create DAO:
```go
type PersonDAO struct {
cqlxoDAO.DAO
}

func mPersonDAO(session gocqlx.Session) (*PersonDAO, error) {
    d := &PersonDAO{}
    err := d.InitDAO(session, Person{})
    
    return d, err
}
```
Save to DB:
```go
person := Person{
    Id:        gocql.TimeUUID(),
    LastName:  "test",
    FirstName: "test2",
    CreatedAt: time.Now(),
}
personDAO, err := mPersonDAO(session)
err = personDAO.Save(person)
```

Load from db:
```go
var persons []Person
err = personDAO.FindAll(&persons)
```

## Examples

You can find lots of examples in:
* [01_example_test.go](/test/01_example_test.go).
* [02_example_test.go](/test/02_example_test.go).
* [03_example_test.go](/test/03_example_test.go).