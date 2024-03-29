package cqlxoCodec

import (
	"errors"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/gocql/gocql"
	"github.com/saivnct/gocqlx-orm/entity"
	"github.com/saivnct/gocqlx-orm/utils/sliceUtils"
	"github.com/scylladb/go-reflectx"
	"github.com/scylladb/gocqlx/v2/table"
	"reflect"
	"slices"
	"strconv"
	"strings"
)

var (
	InvalidTableNameErr        = errors.New("invalid Table Name")
	NoColumnErr                = errors.New("no Column")
	ConflictColumnNameErr      = errors.New("conflict Column Name")
	InvalidPartitionKeyErr     = errors.New("invalid Partition Key")
	NoPartitionKeyErr          = errors.New("no Partition Key")
	InvalidClusterKeyErr       = errors.New("invalid Cluster Key")
	ConflictUDTFieldNameErr    = errors.New("conflict UDT Field Name")
	ConvertToDefaultCQLTypeErr = errors.New("failed to convert to CQL Type")
	ParseCQLTypeErr            = errors.New("failed to parse to CQL Type")
	ParseGoTypeErr             = errors.New("failed to parse to Go Type")
	NotMatchTypesErr           = errors.New("go Type Not Matching CQL Type")
)

func ParseTableMetaData(m cqlxoEntity.BaseModelInterface) (EntityInfo, error) {
	var entityInfo EntityInfo

	tableName := m.TableName()
	if len(m.TableName()) == 0 {
		return entityInfo, InvalidTableNameErr
	}

	// grab the indirected value of entity
	v := reflect.ValueOf(m)
	for v = reflect.ValueOf(m); v.Kind() == reflect.Ptr; {
		v = v.Elem()
	}

	t := v.Type()

	var columns []ColumnInfo
	var indexes []string
	var columFieldMap = map[string]string{}
	var fieldColumdMap = map[string]string{}

	pKeyMap := map[int]string{}
	cKeyMap := map[int]string{}

	maxPkeyIndex := 0
	maxCkeyIndex := 0

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" { //ignore unexported field
			continue
		}

		colName := strings.TrimSpace(field.Tag.Get("db"))
		if colName == "-" {
			continue
		}
		if len(colName) == 0 {
			colName = reflectx.CamelToSnakeASCII(field.Name)
		}

		var cqlType gocql.TypeInfo
		var err error

		frozen := false

		colType := strings.TrimSpace(field.Tag.Get("dbType"))
		if len(colType) == 0 {
			cqlType, err = convertToDefaultCqlType(field.Type)
			if err != nil {
				return entityInfo, fmt.Errorf("%w -> table: %s, field: %s -> %w", ConvertToDefaultCQLTypeErr, tableName, field.Name, err)
			}

			if cqlUDT, ok := cqlType.(gocql.UDTTypeInfo); ok {
				for _, element := range cqlUDT.Elements {
					if element.Type.Type() == gocql.TypeCustom ||
						element.Type.Type() == gocql.TypeUDT ||
						element.Type.Type() == gocql.TypeTuple ||
						element.Type.Type() == gocql.TypeList ||
						element.Type.Type() == gocql.TypeSet ||
						element.Type.Type() == gocql.TypeMap {
						frozen = true
						break
					}
				}
			}

		} else {
			frozen = strings.HasPrefix(colType, "frozen<")

			cqlType, err = getCqlType(colType)
			if err != nil {
				return entityInfo, fmt.Errorf("%w -> table: %s, field: %s -> %w", ParseCQLTypeErr, tableName, field.Name, err)
			}

			if cqlType.Type() == gocql.TypeCustom {
				reCheckCqlType, err := convertToDefaultCqlType(field.Type)
				if err != nil {
					return entityInfo, fmt.Errorf("%w -> table: %s, field: %s -> %w", ConvertToDefaultCQLTypeErr, tableName, field.Name, err)
				}

				if reCheckCqlType.Type() != gocql.TypeCustom {
					if cqlUDT, ok := reCheckCqlType.(gocql.UDTTypeInfo); ok {
						cqlUDT.Name = cqlType.Custom()
						cqlType = cqlUDT
					} else {
						cqlType = reCheckCqlType
					}
				}

				spew.Dump("custom cqlType:", cqlType)

			}

			if !validateFieldType(field.Type, cqlType) {
				return entityInfo, fmt.Errorf("%w -> table: %s, field: %s", NotMatchTypesErr, tableName, field.Name)
			}
		}

		//log.Printf("Field Name: %s,\t Field Type: %s,\t Field Value: %s\n%s\n", field.Name, field.Type, v.Field(i).Interface(), GetCqlTypeInfo(cqlType))
		//log.Printf("(%s-%s): %s\n", field.Name, field.Type, GetCqlTypeInfo(cqlType))

		idx := slices.IndexFunc(columns, func(c ColumnInfo) bool { return c.Name == colName })
		if idx >= 0 {
			return entityInfo, fmt.Errorf("%w -> table: %s, column name: %s", ConflictColumnNameErr, tableName, colName)
		}

		columns = append(columns, ColumnInfo{
			Name:   colName,
			Frozen: frozen,
			Type:   cqlType,
		})
		columFieldMap[colName] = field.Name
		fieldColumdMap[field.Name] = colName

		index := strings.TrimSpace(field.Tag.Get("index"))
		if index == "true" {
			indexes = append(indexes, colName)
		}

		//log.Printf("colName: %s, field.Name: %s\n", colName, field.Name)

		pk := strings.TrimSpace(field.Tag.Get("pk"))
		if len(pk) > 0 {
			pkIndex, err := strconv.Atoi(pk)
			if err != nil {
				return entityInfo, fmt.Errorf("%w -> table: %s, field: %s -> %w", InvalidPartitionKeyErr, tableName, field.Name, err)
			}
			if pkIndex <= 0 {
				return entityInfo, fmt.Errorf("%w -> table: %s, field: %s -> wrong index format", InvalidPartitionKeyErr, tableName, field.Name)
			}
			pKeyMap[pkIndex] = colName
			if pkIndex > maxPkeyIndex {
				maxPkeyIndex = pkIndex
			}
		}

		ck := strings.TrimSpace(field.Tag.Get("ck"))
		if len(ck) > 0 {
			ckIndex, err := strconv.Atoi(ck)
			if err != nil {
				return entityInfo, fmt.Errorf("%w -> table: %s, field: %s -> %w", InvalidClusterKeyErr, tableName, field.Name, err)
			}
			if ckIndex <= 0 {
				return entityInfo, fmt.Errorf("%w -> table: %s, field: %s -> wrong index format", InvalidClusterKeyErr, tableName, field.Name)
			}
			cKeyMap[ckIndex] = colName
			if ckIndex > maxCkeyIndex {
				maxCkeyIndex = ckIndex
			}
		}
	}

	if len(columns) == 0 {
		return entityInfo, NoColumnErr
	}

	if len(pKeyMap) == 0 {
		return entityInfo, NoPartitionKeyErr
	}

	var pkeys []string
	for i := 1; i <= maxPkeyIndex; i++ {
		if pk, ok := pKeyMap[i]; ok {
			pkeys = append(pkeys, pk)
		} else {
			return entityInfo, fmt.Errorf("%w -> table: %s, no column for index %d", InvalidPartitionKeyErr, tableName, i)
		}
	}

	var ckeys []string
	for i := 1; i <= maxCkeyIndex; i++ {
		if ck, ok := cKeyMap[i]; ok {
			ckeys = append(ckeys, ck)
		} else {
			return entityInfo, fmt.Errorf("%w -> table: %s, no column for index %d", InvalidClusterKeyErr, tableName, i)
		}
	}

	tableMetaData := table.Metadata{
		Name:    tableName,
		Columns: sliceUtils.Map(columns, func(c ColumnInfo) string { return c.Name }),
		PartKey: pkeys,
		SortKey: ckeys,
	}

	entityInfo = EntityInfo{
		TableMetaData:  tableMetaData,
		Table:          table.New(tableMetaData),
		Columns:        columns,
		Indexes:        indexes,
		ColumFieldMap:  columFieldMap,
		FieldColumdMap: fieldColumdMap,
	}

	return entityInfo, nil
}

func validateFieldType(fieldType reflect.Type, cqlType gocql.TypeInfo) bool {
	for fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}

	switch cqlType.Type() {
	case gocql.TypeCustom, gocql.TypeUDT, gocql.TypeTuple:
		if fieldType.Kind() != reflect.Struct {
			return false
		}
	case gocql.TypeList, gocql.TypeSet:
		if fieldType.Kind() != reflect.Slice && fieldType.Kind() != reflect.Array {
			return false
		}
		if cqlCollection, ok := cqlType.(gocql.CollectionType); ok {
			return validateFieldType(fieldType.Elem(), cqlCollection.Elem)
		} else {
			return false
		}

	case gocql.TypeMap:
		if fieldType.Kind() != reflect.Map {
			return false
		}
		if cqlCollection, ok := cqlType.(gocql.CollectionType); ok {
			return validateFieldType(fieldType.Elem(), cqlCollection.Elem) && validateFieldType(fieldType.Key(), cqlCollection.Key)
		} else {
			return false
		}
	default:
		gotyp, err := goType(cqlType)
		if err != nil {
			return false
		}

		if gotyp != fieldType {
			return false
		}
	}

	return true
}
