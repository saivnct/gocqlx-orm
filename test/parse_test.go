package test

import (
	"github.com/scylladb/gocqlx/v2"
	"log"
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	f := FavoritePlace{}
	v := reflect.ValueOf(f)
	typ := v.Type()
	log.Println(typ.Implements(reflect.TypeOf((*gocqlx.UDT)(nil)).Elem()))

	var tVal reflect.Value = reflect.New(typ)
	if baseUDT, ok := tVal.Elem().Interface().(gocqlx.UDT); ok {
		log.Println("baseUDT", baseUDT)
	}

	//l := LandMark{}
	//v = reflect.ValueOf(l)
	//typ = v.Type()
	//log.Println(typ.Implements(reflect.TypeOf((*gocqlx.UDT)(nil)).Elem()))
}
