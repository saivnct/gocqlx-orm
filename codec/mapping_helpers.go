package cqlxoCodec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-reflectx"
)

type mappedValue struct {
	value  reflect.Value
	mapper *reflectx.Mapper
}

// MarshalCQL marshals complex values using the library's tag-aware recursive mapper.
func (m *mappedValue) MarshalCQL(info gocql.TypeInfo) ([]byte, error) {
	return marshalMappedValue(info, m.value, m.mapper)
}

// UnmarshalCQL unmarshals complex values using the library's tag-aware recursive mapper.
func (m *mappedValue) UnmarshalCQL(info gocql.TypeInfo, data []byte) error {
	return unmarshalMappedValue(info, data, m.value, m.mapper)
}

// WrapValueForWrite wraps values that need custom recursive mapping before being sent to Scylla.
func WrapValueForWrite(info gocql.TypeInfo, v reflect.Value, mapper *reflectx.Mapper) interface{} {
	if info == nil {
		return v.Interface()
	}
	if needsMappedValue(info) {
		return &mappedValue{value: v, mapper: mapper}
	}
	return v.Interface()
}

// WrapDestForRead wraps scan destinations that need custom recursive mapping on read.
func WrapDestForRead(info gocql.TypeInfo, v reflect.Value, mapper *reflectx.Mapper) interface{} {
	if info == nil {
		return v.Addr().Interface()
	}
	if needsMappedValue(info) {
		return &mappedValue{value: v, mapper: mapper}
	}
	return v.Addr().Interface()
}

// TupleStructToArgs expands a tuple struct into ordered query arguments, preserving nested mapping.
func TupleStructToArgs(v reflect.Value, tupleType gocql.TupleTypeInfo, mapper *reflectx.Mapper) ([]interface{}, error) {
	fields, err := tupleElementValues(v, len(tupleType.Elems))
	if err != nil {
		return nil, err
	}

	elems := make([]interface{}, 0, len(fields))
	for i, field := range fields {
		elems = append(elems, WrapValueForWrite(tupleType.Elems[i], field, mapper))
	}
	return elems, nil
}

// TupleFieldPointers expands a tuple struct into ordered scan destinations for tuple reads.
func TupleFieldPointers(v reflect.Value, tupleType gocql.TupleTypeInfo, mapper *reflectx.Mapper) ([]interface{}, error) {
	fields, err := tupleElementValues(v, len(tupleType.Elems))
	if err != nil {
		return nil, err
	}

	dest := make([]interface{}, 0, len(fields))
	for i, field := range fields {
		dest = append(dest, WrapDestForRead(tupleType.Elems[i], field, mapper))
	}
	return dest, nil
}

// needsMappedValue reports whether a type contains UDT-aware structures that cannot rely on raw gocql mapping.
func needsMappedValue(info gocql.TypeInfo) bool {
	switch info.Type() {
	case gocql.TypeUDT, gocql.TypeCustom:
		return true
	case gocql.TypeList, gocql.TypeSet, gocql.TypeMap:
		collection, ok := info.(gocql.CollectionType)
		if !ok {
			return false
		}
		if collection.Key != nil && needsMappedValue(collection.Key) {
			return true
		}
		return needsMappedValue(collection.Elem)
	case gocql.TypeTuple:
		tuple, ok := info.(gocql.TupleTypeInfo)
		if !ok {
			return false
		}
		for _, elem := range tuple.Elems {
			if needsMappedValue(elem) {
				return true
			}
		}
	}
	return false
}

// tupleElementValues returns the exported fields of a tuple struct in declaration order.
func tupleElementValues(v reflect.Value, expected int) ([]reflect.Value, error) {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, errors.New("tuple value must be a struct")
	}

	var fields []reflect.Value
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}
		fields = append(fields, v.Field(i))
	}

	if len(fields) != expected {
		return nil, fmt.Errorf("expected %d tuple fields, got %d", expected, len(fields))
	}

	return fields, nil
}

// marshalMappedValue recursively marshals tuples, UDTs, and collections using db-tag field mapping.
func marshalMappedValue(info gocql.TypeInfo, v reflect.Value, mapper *reflectx.Mapper) ([]byte, error) {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, nil
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return nil, nil
	}

	switch info.Type() {
	case gocql.TypeUDT, gocql.TypeCustom:
		udt, ok := info.(gocql.UDTTypeInfo)
		if !ok || v.Kind() != reflect.Struct {
			return gocql.Marshal(info, v.Interface())
		}
		fieldMap := mapper.FieldMap(v)
		buf := &bytes.Buffer{}
		for _, elem := range udt.Elements {
			var data []byte
			if field, ok := fieldMap[elem.Name]; ok {
				var err error
				data, err = marshalMappedValue(elem.Type, field, mapper)
				if err != nil {
					return nil, err
				}
			}
			writeBytes(buf, data)
		}
		return buf.Bytes(), nil
	case gocql.TypeList, gocql.TypeSet:
		collection, ok := info.(gocql.CollectionType)
		if !ok || (v.Kind() != reflect.Slice && v.Kind() != reflect.Array) {
			return gocql.Marshal(info, v.Interface())
		}
		if v.Kind() == reflect.Slice && v.IsNil() {
			return nil, nil
		}
		buf := &bytes.Buffer{}
		writeInt32(buf, v.Len())
		for i := 0; i < v.Len(); i++ {
			data, err := marshalMappedValue(collection.Elem, v.Index(i), mapper)
			if err != nil {
				return nil, err
			}
			writeBytes(buf, data)
		}
		return buf.Bytes(), nil
	case gocql.TypeMap:
		collection, ok := info.(gocql.CollectionType)
		if !ok || v.Kind() != reflect.Map {
			return gocql.Marshal(info, v.Interface())
		}
		if v.IsNil() {
			return nil, nil
		}
		buf := &bytes.Buffer{}
		keys := v.MapKeys()
		writeInt32(buf, len(keys))
		for _, key := range keys {
			keyData, err := marshalMappedValue(collection.Key, key, mapper)
			if err != nil {
				return nil, err
			}
			valData, err := marshalMappedValue(collection.Elem, v.MapIndex(key), mapper)
			if err != nil {
				return nil, err
			}
			writeBytes(buf, keyData)
			writeBytes(buf, valData)
		}
		return buf.Bytes(), nil
	case gocql.TypeTuple:
		tuple, ok := info.(gocql.TupleTypeInfo)
		if !ok {
			return gocql.Marshal(info, v.Interface())
		}
		fields, err := tupleElementValues(v, len(tuple.Elems))
		if err != nil {
			return nil, err
		}
		buf := &bytes.Buffer{}
		for i, field := range fields {
			data, err := marshalMappedValue(tuple.Elems[i], field, mapper)
			if err != nil {
				return nil, err
			}
			writeBytes(buf, data)
		}
		return buf.Bytes(), nil
	default:
		return gocql.Marshal(info, v.Interface())
	}
}

// unmarshalMappedValue recursively unmarshals tuples, UDTs, and collections using db-tag field mapping.
func unmarshalMappedValue(info gocql.TypeInfo, data []byte, v reflect.Value, mapper *reflectx.Mapper) error {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return errors.New("invalid destination value")
	}

	if data == nil {
		if v.CanSet() {
			v.Set(reflect.Zero(v.Type()))
		}
		return nil
	}

	switch info.Type() {
	case gocql.TypeUDT, gocql.TypeCustom:
		udt, ok := info.(gocql.UDTTypeInfo)
		if !ok || v.Kind() != reflect.Struct {
			return gocql.Unmarshal(info, data, v.Addr().Interface())
		}
		fieldMap := mapper.FieldMap(v)
		for _, elem := range udt.Elements {
			p, rest, err := readBytes(data)
			if err != nil {
				return err
			}
			data = rest
			if field, ok := fieldMap[elem.Name]; ok {
				if err := unmarshalMappedValue(elem.Type, p, field, mapper); err != nil {
					return err
				}
			}
		}
		return nil
	case gocql.TypeList, gocql.TypeSet:
		collection, ok := info.(gocql.CollectionType)
		if !ok || (v.Kind() != reflect.Slice && v.Kind() != reflect.Array) {
			return gocql.Unmarshal(info, data, v.Addr().Interface())
		}
		size, rest, err := readCollectionCount(data)
		if err != nil {
			return err
		}
		data = rest
		if v.Kind() == reflect.Array {
			if v.Len() != size {
				return fmt.Errorf("array with wrong size: expected %d, got %d", v.Len(), size)
			}
		} else {
			v.Set(reflect.MakeSlice(v.Type(), size, size))
		}
		for i := 0; i < size; i++ {
			item, rest, err := readBytes(data)
			if err != nil {
				return err
			}
			data = rest
			if err := unmarshalMappedValue(collection.Elem, item, v.Index(i), mapper); err != nil {
				return err
			}
		}
		return nil
	case gocql.TypeMap:
		collection, ok := info.(gocql.CollectionType)
		if !ok || v.Kind() != reflect.Map {
			return gocql.Unmarshal(info, data, v.Addr().Interface())
		}
		size, rest, err := readCollectionCount(data)
		if err != nil {
			return err
		}
		data = rest
		v.Set(reflect.MakeMapWithSize(v.Type(), size))
		for i := 0; i < size; i++ {
			keyData, rest, err := readBytes(data)
			if err != nil {
				return err
			}
			data = rest
			valData, rest, err := readBytes(data)
			if err != nil {
				return err
			}
			data = rest

			key := reflect.New(v.Type().Key()).Elem()
			if err := unmarshalMappedValue(collection.Key, keyData, key, mapper); err != nil {
				return err
			}
			val := reflect.New(v.Type().Elem()).Elem()
			if err := unmarshalMappedValue(collection.Elem, valData, val, mapper); err != nil {
				return err
			}
			v.SetMapIndex(key, val)
		}
		return nil
	case gocql.TypeTuple:
		tuple, ok := info.(gocql.TupleTypeInfo)
		if !ok {
			return gocql.Unmarshal(info, data, v.Addr().Interface())
		}
		fields, err := tupleElementValues(v, len(tuple.Elems))
		if err != nil {
			return err
		}
		for i, field := range fields {
			item, rest, err := readBytes(data)
			if err != nil {
				return err
			}
			data = rest
			if err := unmarshalMappedValue(tuple.Elems[i], item, field, mapper); err != nil {
				return err
			}
		}
		return nil
	default:
		return gocql.Unmarshal(info, data, v.Addr().Interface())
	}
}

// writeInt32 encodes a signed 32-bit length/value using the collection/tuple wire format.
func writeInt32(buf *bytes.Buffer, n int) {
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], uint32(int32(n)))
	buf.Write(raw[:])
}

// writeBytes writes a length-prefixed value, using -1 for nil payloads.
func writeBytes(buf *bytes.Buffer, data []byte) {
	if data == nil {
		writeInt32(buf, -1)
		return
	}
	writeInt32(buf, len(data))
	buf.Write(data)
}

// readCollectionCount reads the element count prefix used by lists, sets, and maps.
func readCollectionCount(data []byte) (int, []byte, error) {
	if len(data) < 4 {
		return 0, nil, fmt.Errorf("unexpected eof while reading collection size")
	}
	size := int(int32(binary.BigEndian.Uint32(data[:4])))
	return size, data[4:], nil
}

// readBytes reads a length-prefixed payload, returning nil when the encoded size is -1.
func readBytes(data []byte) ([]byte, []byte, error) {
	if len(data) < 4 {
		return nil, nil, fmt.Errorf("unexpected eof while reading bytes length")
	}
	size := int(int32(binary.BigEndian.Uint32(data[:4])))
	data = data[4:]
	if size < 0 {
		return nil, data, nil
	}
	if len(data) < size {
		return nil, nil, fmt.Errorf("unexpected eof while reading bytes payload")
	}
	return data[:size], data[size:], nil
}
