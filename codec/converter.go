package cqlxoCodec

import (
	"fmt"
	"giangbb.studio/go.cqlx.orm/entity"
	"github.com/gocql/gocql"
	"github.com/scylladb/go-reflectx"
	"gopkg.in/inf.v0"
	"math/big"
	"reflect"
	"slices"
	"strings"
	"time"
)

// convertToDefaultCqlType - Convert from go Type to CQL Type - based on gocql -> helpers.go -> goType()
func convertToDefaultCqlType(t reflect.Type) (gocql.TypeInfo, error) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t {
	case reflect.TypeOf(*new(string)):
		return gocql.NewNativeType(5, gocql.TypeText, ""), nil
	case reflect.TypeOf(*new(time.Duration)):
		return gocql.NewNativeType(5, gocql.TypeDuration, ""), nil
	case reflect.TypeOf(*new(time.Time)):
		return gocql.NewNativeType(5, gocql.TypeTimestamp, ""), nil
	case reflect.TypeOf(*new([]byte)):
		return gocql.NewNativeType(5, gocql.TypeBlob, ""), nil
	case reflect.TypeOf(*new(bool)):
		return gocql.NewNativeType(5, gocql.TypeBoolean, ""), nil
	case reflect.TypeOf(*new(float32)):
		return gocql.NewNativeType(5, gocql.TypeFloat, ""), nil
	case reflect.TypeOf(*new(float64)):
		return gocql.NewNativeType(5, gocql.TypeDouble, ""), nil
	case reflect.TypeOf(*new(int)):
		return gocql.NewNativeType(5, gocql.TypeInt, ""), nil
	case reflect.TypeOf(*new(int64)):
		return gocql.NewNativeType(5, gocql.TypeBigInt, ""), nil
	case reflect.TypeOf(*new(*big.Int)):
		return gocql.NewNativeType(5, gocql.TypeVarint, ""), nil
	case reflect.TypeOf(*new(int16)):
		return gocql.NewNativeType(5, gocql.TypeSmallInt, ""), nil
	case reflect.TypeOf(*new(int8)):
		return gocql.NewNativeType(5, gocql.TypeTinyInt, ""), nil
	case reflect.TypeOf(*new(*inf.Dec)):
		return gocql.NewNativeType(5, gocql.TypeDecimal, ""), nil
	case reflect.TypeOf(*new(gocql.UUID)):
		return gocql.NewNativeType(5, gocql.TypeUUID, ""), nil
	default:
		{
			if t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
				elemTypeInfo, err := convertToDefaultCqlType(t.Elem())
				typeInfo := gocql.CollectionType{
					NativeType: gocql.NewNativeType(5, gocql.TypeList, ""),
					Elem:       elemTypeInfo,
				}
				return typeInfo, err
			} else if t.Kind() == reflect.Map {
				typeInfo := &gocql.CollectionType{
					NativeType: gocql.NewNativeType(5, gocql.TypeMap, ""),
				}
				keyTypeInfo, err := convertToDefaultCqlType(t.Key())
				if err != nil {
					return typeInfo, err
				}
				elemTypeInfo, err := convertToDefaultCqlType(t.Elem())
				if err != nil {
					return typeInfo, err
				}

				typeInfo.Key = keyTypeInfo
				typeInfo.Elem = elemTypeInfo
				return *typeInfo, nil
			} else if t.Implements(reflect.TypeOf((*cqlxoEntity.BaseUDTInterface)(nil)).Elem()) {
				var tVal reflect.Value = reflect.New(t)
				if baseUDT, ok := tVal.Elem().Interface().(cqlxoEntity.BaseUDTInterface); ok {
					typeInfo := &gocql.UDTTypeInfo{
						NativeType: gocql.NewNativeType(5, gocql.TypeUDT, ""),
						Name:       baseUDT.UDTName(),
					}
					udtFields, err := convertToCqlUDT(baseUDT)
					if err != nil {
						return typeInfo, err
					}
					typeInfo.Elements = udtFields
					return *typeInfo, nil
				}
			} else if t.Implements(reflect.TypeOf((*cqlxoEntity.BaseTupleInterface)(nil)).Elem()) {
				var tVal reflect.Value = reflect.New(t)
				if baseTuple, ok := tVal.Elem().Interface().(cqlxoEntity.BaseTupleInterface); ok {
					typeInfo := &gocql.TupleTypeInfo{
						NativeType: gocql.NewNativeType(5, gocql.TypeTuple, ""),
					}

					tupleFields, err := convertToCqlTuple(baseTuple)
					if err != nil {
						return typeInfo, err
					}
					typeInfo.Elems = tupleFields
					return *typeInfo, nil
				}
			}
			return gocql.NewNativeType(0, gocql.TypeCustom, ""), fmt.Errorf("cannot create Go type for unknown CQL type %s", t)
		}

	}
}

// convertToCqlUDT - Convert from go UDT type to CQL UDT Fields type
func convertToCqlUDT(m cqlxoEntity.BaseUDTInterface) ([]gocql.UDTField, error) {
	t := reflect.TypeOf(m)

	var udtFields []gocql.UDTField
	var fieldNames []string

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" { //ignore unexported field
			continue
		}

		fieldName := strings.TrimSpace(field.Tag.Get("db"))
		if fieldName == "-" {
			continue
		}
		if len(fieldName) == 0 {
			fieldName = reflectx.CamelToSnakeASCII(field.Name)
		}

		idx := slices.IndexFunc(fieldNames, func(c string) bool { return c == fieldName })
		if idx >= 0 {
			return nil, fmt.Errorf("%w -> field name: %s", ConflictUDTFieldNameErr, fieldName)
		}
		fieldNames = append(fieldNames, fieldName)

		cqlType, err := convertToDefaultCqlType(field.Type)
		if err != nil {
			return nil, err
		}

		udtFields = append(udtFields, gocql.UDTField{
			Name: fieldName,
			Type: cqlType,
		})
	}

	return udtFields, nil
}

// convertToCqlTuple - Convert from go struct type to CQL Tuple Elements type
func convertToCqlTuple(m cqlxoEntity.BaseTupleInterface) ([]gocql.TypeInfo, error) {
	t := reflect.TypeOf(m)

	var tupleFields []gocql.TypeInfo

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" { //ignore unexported field
			continue
		}

		cqlType, err := convertToDefaultCqlType(field.Type)
		if err != nil {
			return nil, err
		}

		tupleFields = append(tupleFields, cqlType)
	}

	return tupleFields, nil
}

func goType(t gocql.TypeInfo) (reflect.Type, error) {
	switch t.Type() {
	case gocql.TypeVarchar, gocql.TypeAscii, gocql.TypeInet, gocql.TypeText:
		return reflect.TypeOf(*new(string)), nil
	case gocql.TypeBigInt, gocql.TypeCounter:
		return reflect.TypeOf(*new(int64)), nil
	case gocql.TypeTime:
		return reflect.TypeOf(*new(time.Duration)), nil
	case gocql.TypeTimestamp:
		return reflect.TypeOf(*new(time.Time)), nil
	case gocql.TypeBlob:
		return reflect.TypeOf(*new([]byte)), nil
	case gocql.TypeBoolean:
		return reflect.TypeOf(*new(bool)), nil
	case gocql.TypeFloat:
		return reflect.TypeOf(*new(float32)), nil
	case gocql.TypeDouble:
		return reflect.TypeOf(*new(float64)), nil
	case gocql.TypeInt:
		return reflect.TypeOf(*new(int)), nil
	case gocql.TypeSmallInt:
		return reflect.TypeOf(*new(int16)), nil
	case gocql.TypeTinyInt:
		return reflect.TypeOf(*new(int8)), nil
	case gocql.TypeDecimal:
		return reflect.TypeOf(*new(*inf.Dec)), nil
	case gocql.TypeUUID, gocql.TypeTimeUUID:
		return reflect.TypeOf(*new(gocql.UUID)), nil
	case gocql.TypeList, gocql.TypeSet:
		elemType, err := goType(t.(gocql.CollectionType).Elem)
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(elemType), nil
	case gocql.TypeMap:
		keyType, err := goType(t.(gocql.CollectionType).Key)
		if err != nil {
			return nil, err
		}
		valueType, err := goType(t.(gocql.CollectionType).Elem)
		if err != nil {
			return nil, err
		}
		return reflect.MapOf(keyType, valueType), nil
	case gocql.TypeVarint:
		return reflect.TypeOf(*new(*big.Int)), nil
	case gocql.TypeTuple:
		// what can we do here? all there is to do is to make a list of interface{}
		tuple := t.(gocql.TupleTypeInfo)
		return reflect.TypeOf(make([]interface{}, len(tuple.Elems))), nil
	case gocql.TypeUDT:
		return reflect.TypeOf(make(map[string]interface{})), nil
	case gocql.TypeDate:
		return reflect.TypeOf(*new(time.Time)), nil
	case gocql.TypeDuration:
		return reflect.TypeOf(*new(gocql.Duration)), nil
	default:
		return nil, fmt.Errorf("cannot create Go type for unknown CQL type %s", t)
	}
}

// getCqlBaseType - get CQL base type from name
func getCqlBaseType(name string) gocql.Type {
	switch name {
	case "ascii":
		return gocql.TypeAscii
	case "bigint":
		return gocql.TypeBigInt
	case "blob":
		return gocql.TypeBlob
	case "boolean":
		return gocql.TypeBoolean
	case "counter":
		return gocql.TypeCounter
	case "date":
		return gocql.TypeDate
	case "decimal":
		return gocql.TypeDecimal
	case "double":
		return gocql.TypeDouble
	case "duration":
		return gocql.TypeDuration
	case "float":
		return gocql.TypeFloat
	case "int":
		return gocql.TypeInt
	case "smallint":
		return gocql.TypeSmallInt
	case "tinyint":
		return gocql.TypeTinyInt
	case "time":
		return gocql.TypeTime
	case "timestamp":
		return gocql.TypeTimestamp
	case "uuid":
		return gocql.TypeUUID
	case "varchar":
		return gocql.TypeVarchar
	case "text":
		return gocql.TypeText
	case "varint":
		return gocql.TypeVarint
	case "timeuuid":
		return gocql.TypeTimeUUID
	case "inet":
		return gocql.TypeInet
	case "MapType":
		return gocql.TypeMap
	case "ListType":
		return gocql.TypeList
	case "SetType":
		return gocql.TypeSet
	case "TupleType":
		return gocql.TypeTuple
	default:
		return gocql.TypeCustom
	}
}

// getCqlBaseType - get CQL type from name
func getCqlType(name string) (gocql.TypeInfo, error) {
	if strings.HasPrefix(name, "frozen<") {
		return getCqlType(strings.TrimPrefix(name[:len(name)-1], "frozen<"))
	} else if strings.HasPrefix(name, "set<") {
		elemTypeInfo, err := getCqlType(strings.TrimPrefix(name[:len(name)-1], "set<"))
		typeInfo := gocql.CollectionType{
			NativeType: gocql.NewNativeType(5, gocql.TypeSet, ""),
			Elem:       elemTypeInfo,
		}
		return typeInfo, err
	} else if strings.HasPrefix(name, "list<") {
		elemTypeInfo, err := getCqlType(strings.TrimPrefix(name[:len(name)-1], "list<"))
		return gocql.CollectionType{
			NativeType: gocql.NewNativeType(5, gocql.TypeList, ""),
			Elem:       elemTypeInfo,
		}, err
	} else if strings.HasPrefix(name, "map<") {
		names := splitCompositeTypes(strings.TrimPrefix(name[:len(name)-1], "map<"))
		if len(names) != 2 {
			return gocql.NewNativeType(0, gocql.TypeCustom, ""), fmt.Errorf("Error parsing map type, it has %d subelements, expecting 2\n", len(names))
		}

		typeInfo := &gocql.CollectionType{
			NativeType: gocql.NewNativeType(5, gocql.TypeMap, ""),
		}
		keyTypeInfo, err := getCqlType(names[0])
		if err != nil {
			return typeInfo, err
		}
		elemTypeInfo, err := getCqlType(names[1])
		if err != nil {
			return typeInfo, err
		}

		typeInfo.Key = keyTypeInfo
		typeInfo.Elem = elemTypeInfo

		return *typeInfo, nil
	} else if strings.HasPrefix(name, "tuple<") {
		typeInfo := &gocql.TupleTypeInfo{
			NativeType: gocql.NewNativeType(5, gocql.TypeTuple, ""),
		}

		names := splitCompositeTypes(strings.TrimPrefix(name[:len(name)-1], "tuple<"))
		types := make([]gocql.TypeInfo, len(names))
		for i, name := range names {
			var err error
			types[i], err = getCqlType(name)
			if err != nil {
				return typeInfo, err
			}
		}

		typeInfo.Elems = types

		return *typeInfo, nil
	} else {
		var typeInfo gocql.TypeInfo

		typ := getCqlBaseType(name)
		if typ == gocql.TypeCustom {
			typeInfo = gocql.NewNativeType(0, getCqlBaseType(name), name)
		} else {
			typeInfo = gocql.NewNativeType(0, getCqlBaseType(name), "")
		}

		return typeInfo, nil
	}
}

func splitCompositeTypes(name string) []string {
	if !strings.Contains(name, "<") {
		return strings.Split(name, ", ")
	}
	var parts []string
	lessCount := 0
	segment := ""
	for _, char := range name {
		if char == ',' && lessCount == 0 {
			if segment != "" {
				parts = append(parts, strings.TrimSpace(segment))
			}
			segment = ""
			continue
		}
		segment += string(char)
		if char == '<' {
			lessCount++
		} else if char == '>' {
			lessCount--
		}
	}
	if segment != "" {
		parts = append(parts, strings.TrimSpace(segment))
	}
	return parts
}
