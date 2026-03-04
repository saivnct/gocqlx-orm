package cqlxoCodec

import (
	"reflect"
	"testing"

	"github.com/gocql/gocql"
)

type blobEntity struct {
	ID       gocql.UUID `db:"id" pk:"1"`
	Payload  []byte     `db:"payload"`
	Checksum []byte     `db:"checksum" dbType:"blob"`
}

func (blobEntity) TableName() string { return "blob_entity" }

func TestParseTableMetaData_ByteArrayInferredAsBlob(t *testing.T) {
	entityInfo, err := ParseTableMetaData(blobEntity{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	columnDecls := map[string]string{}
	for _, c := range entityInfo.Columns {
		columnDecls[c.Name] = c.GetCqlTypeDeclareStatement()
	}

	if columnDecls["payload"] != "payload blob" {
		t.Fatalf("expected payload blob, got %q", columnDecls["payload"])
	}
	if columnDecls["checksum"] != "checksum blob" {
		t.Fatalf("expected checksum blob, got %q", columnDecls["checksum"])
	}
}

func TestConvertToDefaultCqlType_ByteArrayIsBlob(t *testing.T) {
	cqlType, err := convertToDefaultCqlType(reflectTypeOfByteArray())
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if cqlType.Type() != gocql.TypeBlob {
		t.Fatalf("expected blob type, got %s", cqlType.Type())
	}
}

func reflectTypeOfByteArray() reflect.Type {
	return reflect.TypeOf([]byte{})
}
