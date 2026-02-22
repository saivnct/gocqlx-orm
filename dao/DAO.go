package cqlxoDAO

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/gocql/gocql"
	"github.com/saivnct/gocqlx-orm/codec"
	"github.com/saivnct/gocqlx-orm/entity"
	"github.com/saivnct/gocqlx-orm/utils/sliceUtils"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
)

var (
	NoSessionError      = errors.New("no Session Connection Found")
	NoQueryEntityError  = errors.New("no Query Entity")
	InvalidPrimaryKey   = errors.New("invalid PrimaryKey")
	InvalidPartitionKey = errors.New("invalid PartitionKey")
)

type DAO struct {
	EntityInfo cqlxoCodec.EntityInfo
	Session    gocqlx.Session
}

type QueryOption struct {
	AllowFiltering bool
	Page           []byte
	ItemsPerPage   int
	OrderBy        string
	Order          qb.Order
}

func (d *DAO) InitDAO(session gocqlx.Session, m cqlxoEntity.BaseScyllaEntityInterface) error {
	entityInfo, err := cqlxoCodec.ParseTableMetaData(m)
	if err != nil {
		return err
	}

	d.EntityInfo = entityInfo
	d.Session = session

	err = d.checkAndCreateUDT()
	if err != nil {
		return err
	}

	err = d.checkAndCreateTable()
	if err != nil {
		return err
	}

	err = d.checkAndCreateIndex()
	if err != nil {
		return err
	}

	//log.Printf("DAO %s created!", m.TableName())
	return nil
}

func (d *DAO) checkAndCreateUDT() error {
	if d.Session.Session == nil {
		return NoSessionError
	}
	udts := d.EntityInfo.ScanUDTs()

	//reverse the order of udts to make sure the nested udt is created before the parent udt
	udts = sliceUtils.Reverse(udts)

	for _, udt := range udts {
		//log.Println(cqlxoCodec.GetCqlCreateUDTStatement(udt))
		err := d.Session.ExecStmt(cqlxoCodec.GetCqlCreateUDTStatement(udt))
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DAO) checkAndCreateTable() error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	//log.Println(d.EntityInfo.GetCreateTableStatement())
	err := d.Session.ExecStmt(d.EntityInfo.GetCreateTableStatement())
	return err
}

func (d *DAO) checkAndCreateIndex() error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	for _, index := range d.EntityInfo.Indexes {
		err := d.Session.ExecStmt(fmt.Sprintf("CREATE INDEX IF NOT EXISTS ON %s(%s)", d.EntityInfo.TableMetaData.Name, index))
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *DAO) Save(entity cqlxoEntity.BaseScyllaEntityInterface) error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	if d.hasTupleColumn() {
		stmt, args, err := d.getInsertStmtAndArgs(entity)
		if err != nil {
			return err
		}
		return d.Session.Session.Query(stmt, args...).Exec()
	}

	q := d.Session.Query(d.EntityInfo.Table.Insert()).BindStruct(entity)
	//log.Printf("Save %s", q.String())
	return q.ExecRelease()
}

func (d *DAO) SaveMany(entities []cqlxoEntity.BaseScyllaEntityInterface) error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	if d.hasTupleColumn() {
		stmt := d.getInsertStmt()
		for _, entity := range entities {
			args, err := d.getInsertArgs(entity)
			if err != nil {
				return err
			}
			if err = d.Session.Session.Query(stmt, args...).Exec(); err != nil {
				return err
			}
		}
		return nil
	}

	q := d.EntityInfo.Table.InsertQuery(d.Session)
	for _, entity := range entities {
		q.BindStruct(entity)
		if err := q.Exec(); err != nil {
			return err
		}
	}
	q.Release()
	return nil
}

func (d *DAO) hasTupleColumn() bool {
	for _, column := range d.EntityInfo.Columns {
		if column.Type.Type() == gocql.TypeTuple {
			return true
		}
	}
	return false
}

func (d *DAO) getInsertStmtAndArgs(entity cqlxoEntity.BaseScyllaEntityInterface) (string, []interface{}, error) {
	stmt := d.getInsertStmt()
	args, err := d.getInsertArgs(entity)
	return stmt, args, err
}

func (d *DAO) getInsertStmt() string {
	columns := d.EntityInfo.TableMetaData.Columns
	var placeholders []string

	for _, colName := range columns {
		colInfo, ok := d.getColumnInfo(colName)
		if !ok || colInfo.Type.Type() != gocql.TypeTuple {
			placeholders = append(placeholders, "?")
			continue
		}

		tupleType := colInfo.Type.(gocql.TupleTypeInfo)
		tuplePlaceholders := strings.Repeat("?,", len(tupleType.Elems))
		placeholders = append(placeholders, fmt.Sprintf("(%s)", strings.TrimSuffix(tuplePlaceholders, ",")))
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		d.EntityInfo.TableMetaData.Name,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "),
	)
}

func (d *DAO) getInsertArgs(entity cqlxoEntity.BaseScyllaEntityInterface) ([]interface{}, error) {
	v := reflect.ValueOf(entity)
	for v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, errors.New("entity must be a struct")
	}

	var args []interface{}
	for _, colName := range d.EntityInfo.TableMetaData.Columns {
		fieldName := d.EntityInfo.ColumFieldMap[colName]
		fieldValue := v.FieldByName(fieldName)

		colInfo, ok := d.getColumnInfo(colName)
		if !ok || colInfo.Type.Type() != gocql.TypeTuple {
			args = append(args, fieldValue.Interface())
			continue
		}

		tupleElems, err := tupleStructToArgs(fieldValue)
		if err != nil {
			return nil, fmt.Errorf("tuple column %s: %w", colName, err)
		}
		tupleType := colInfo.Type.(gocql.TupleTypeInfo)
		if len(tupleElems) != len(tupleType.Elems) {
			return nil, fmt.Errorf("tuple column %s: expected %d fields, got %d", colName, len(tupleType.Elems), len(tupleElems))
		}
		args = append(args, tupleElems...)
	}
	return args, nil
}

func (d *DAO) getColumnInfo(colName string) (cqlxoCodec.ColumnInfo, bool) {
	for _, col := range d.EntityInfo.Columns {
		if col.Name == colName {
			return col, true
		}
	}
	return cqlxoCodec.ColumnInfo{}, false
}

func tupleStructToArgs(v reflect.Value) ([]interface{}, error) {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil, errors.New("tuple value is nil")
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, errors.New("tuple value must be a struct")
	}

	var elems []interface{}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}
		elems = append(elems, v.Field(i).Interface())
	}
	return elems, nil
}

func (d *DAO) selectRelease(q *gocqlx.Queryx, result interface{}) error {
	if !d.hasTupleColumn() {
		return q.SelectRelease(result)
	}
	defer q.Release()
	return d.scanTupleIter(q.Iter(), result)
}

func (d *DAO) scanTupleIter(iter *gocqlx.Iterx, result interface{}) error {
	resultVal := reflect.ValueOf(result)
	if resultVal.Kind() != reflect.Ptr || resultVal.IsNil() {
		return errors.New("result must be a non-nil pointer to slice")
	}

	sliceVal := resultVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return errors.New("result must be a pointer to slice")
	}

	elemType := sliceVal.Type().Elem()
	scanner := iter.Scanner()

	for scanner.Next() {
		elem := reflect.New(elemType).Elem()
		dest, err := d.buildTupleScanDest(elem)
		if err != nil {
			return err
		}
		if err = scanner.Scan(dest...); err != nil {
			return err
		}
		sliceVal = reflect.Append(sliceVal, elem)
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	resultVal.Elem().Set(sliceVal)
	return nil
}

func (d *DAO) buildTupleScanDest(row reflect.Value) ([]interface{}, error) {
	var dest []interface{}

	for _, colName := range d.EntityInfo.TableMetaData.Columns {
		fieldName, ok := d.EntityInfo.ColumFieldMap[colName]
		if !ok || fieldName == "" {
			return nil, fmt.Errorf("column %s has no field mapping", colName)
		}

		fieldVal := row.FieldByName(fieldName)
		if !fieldVal.IsValid() {
			return nil, fmt.Errorf("field %s not found for column %s", fieldName, colName)
		}

		colInfo, ok := d.getColumnInfo(colName)
		if !ok || colInfo.Type.Type() != gocql.TypeTuple {
			dest = append(dest, fieldVal.Addr().Interface())
			continue
		}

		tupleType := colInfo.Type.(gocql.TupleTypeInfo)
		tupleDest, err := tupleFieldPointers(fieldVal, len(tupleType.Elems))
		if err != nil {
			return nil, fmt.Errorf("tuple column %s: %w", colName, err)
		}
		dest = append(dest, tupleDest...)
	}

	return dest, nil
}

func tupleFieldPointers(v reflect.Value, expected int) ([]interface{}, error) {
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			v.Set(reflect.New(v.Type().Elem()))
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return nil, errors.New("tuple destination must be a struct")
	}

	var dest []interface{}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" {
			continue
		}
		dest = append(dest, v.Field(i).Addr().Interface())
	}

	if len(dest) != expected {
		return nil, fmt.Errorf("expected %d tuple fields, got %d", expected, len(dest))
	}

	return dest, nil
}

func (d *DAO) FindAll(result interface{}) error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	q := qb.Select(d.EntityInfo.TableMetaData.Name).Columns(d.EntityInfo.TableMetaData.Columns...).Query(d.Session)
	return d.selectRelease(q, result)
}

func (d *DAO) FindByPrimaryKey(queryEntity cqlxoEntity.BaseScyllaEntityInterface, result interface{}) error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	if queryEntity == nil {
		return NoQueryEntityError
	}

	keys := append(d.EntityInfo.TableMetaData.PartKey, d.EntityInfo.TableMetaData.SortKey...)
	queryMap := d.getQueryMap(queryEntity, keys)

	if len(queryMap) != len(keys) {
		return InvalidPrimaryKey
	}

	var cmps = getCmp(queryMap)
	q := qb.
		Select(d.EntityInfo.TableMetaData.Name).
		Columns(d.EntityInfo.TableMetaData.Columns...).
		Where(cmps...).
		Query(d.Session).
		BindMap(queryMap)

	return d.selectRelease(q, result)
}

func (d *DAO) FindByPartitionKey(queryEntity cqlxoEntity.BaseScyllaEntityInterface, result interface{}) error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	if queryEntity == nil {
		return NoQueryEntityError
	}

	keys := d.EntityInfo.TableMetaData.PartKey
	queryMap := d.getQueryMap(queryEntity, keys)

	if len(queryMap) != len(keys) {
		return InvalidPartitionKey
	}

	//log.Print(queryMap)

	var cmps = getCmp(queryMap)
	q := qb.
		Select(d.EntityInfo.TableMetaData.Name).
		Columns(d.EntityInfo.TableMetaData.Columns...).
		Where(cmps...).
		Query(d.Session).
		BindMap(queryMap)

	return d.selectRelease(q, result)
}

func (d *DAO) Find(queryEntity cqlxoEntity.BaseScyllaEntityInterface, allowFiltering bool, result interface{}) error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	if queryEntity == nil {
		return d.FindAll(result)
	}

	columns := d.EntityInfo.TableMetaData.Columns
	queryMap := d.getQueryMap(queryEntity, columns)

	if len(queryMap) == 0 {
		return d.FindAll(result)
	}

	//log.Print(queryMap)

	var cmps = getCmp(queryMap)

	sBuilder := qb.
		Select(d.EntityInfo.TableMetaData.Name).
		Columns(d.EntityInfo.TableMetaData.Columns...).
		Where(cmps...)

	if allowFiltering {
		sBuilder.AllowFiltering()
	}

	q := sBuilder.Query(d.Session).BindMap(queryMap)

	return d.selectRelease(q, result)
}

func (d *DAO) FindWithOption(queryEntity cqlxoEntity.BaseScyllaEntityInterface, option QueryOption, result interface{}) (nextPage []byte, err error) {
	if d.Session.Session == nil {
		return nil, NoSessionError
	}

	sBuilder := qb.
		Select(d.EntityInfo.TableMetaData.Name).
		Columns(d.EntityInfo.TableMetaData.Columns...)

	var queryMap qb.M
	if queryEntity != nil {
		columns := d.EntityInfo.TableMetaData.Columns
		queryMap = d.getQueryMap(queryEntity, columns)
		cmps := getCmp(queryMap)
		sBuilder = sBuilder.Where(cmps...)
	}

	if option.AllowFiltering {
		sBuilder.AllowFiltering()
	}

	if len(option.OrderBy) > 0 {
		sBuilder.OrderBy(option.OrderBy, option.Order)
	}
	q := sBuilder.Query(d.Session)
	if queryMap != nil {
		q = q.BindMap(queryMap)
	}
	defer q.Release()
	q.PageState(option.Page)
	q.PageSize(option.ItemsPerPage)
	iter := q.Iter()
	next := iter.PageState()
	if d.hasTupleColumn() {
		return next, d.scanTupleIter(iter, result)
	}
	return next, iter.Select(result)
}

func (d *DAO) CountAll() (int64, error) {
	if d.Session.Session == nil {
		return 0, NoSessionError
	}

	var count []int64

	q := qb.Select(d.EntityInfo.TableMetaData.Name).CountAll().Query(d.Session)
	err := q.SelectRelease(&count)
	if err != nil {
		return 0, err
	}
	if len(count) == 0 {
		return 0, nil
	}

	return count[0], err
}

func (d *DAO) Count(queryEntity cqlxoEntity.BaseScyllaEntityInterface, allowFiltering bool) (int64, error) {
	if d.Session.Session == nil {
		return 0, NoSessionError
	}

	if queryEntity == nil {
		return d.CountAll()
	}

	columns := d.EntityInfo.TableMetaData.Columns
	queryMap := d.getQueryMap(queryEntity, columns)

	if len(queryMap) == 0 {
		return d.CountAll()
	}

	var cmps = getCmp(queryMap)

	sBuilder := qb.
		Select(d.EntityInfo.TableMetaData.Name).
		Where(cmps...).CountAll()

	if allowFiltering {
		sBuilder.AllowFiltering()
	}

	var count []int64

	q := sBuilder.Query(d.Session).BindMap(queryMap)
	err := q.SelectRelease(&count)
	if err != nil || len(count) == 0 {
		return 0, err
	}

	return count[0], err
}

func (d *DAO) DeleteAll() error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	return d.Session.ExecStmt(fmt.Sprintf("TRUNCATE %s", d.EntityInfo.TableMetaData.Name))
}

func (d *DAO) DeleteByPrimaryKey(queryEntity cqlxoEntity.BaseScyllaEntityInterface) error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	if queryEntity == nil {
		return NoQueryEntityError
	}

	keys := append(d.EntityInfo.TableMetaData.PartKey, d.EntityInfo.TableMetaData.SortKey...)
	queryMap := d.getQueryMap(queryEntity, keys)

	if len(queryMap) != len(keys) {
		return InvalidPrimaryKey
	}

	var cmps = getCmp(queryMap)

	q := qb.
		Delete(d.EntityInfo.TableMetaData.Name).
		Where(cmps...).
		Query(d.Session).
		BindMap(queryMap)

	return q.ExecRelease()
}

func (d *DAO) DeleteByPartitionKey(queryEntity cqlxoEntity.BaseScyllaEntityInterface) error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	if queryEntity == nil {
		return NoQueryEntityError
	}

	keys := d.EntityInfo.TableMetaData.PartKey
	queryMap := d.getQueryMap(queryEntity, keys)

	if len(queryMap) != len(keys) {
		return InvalidPartitionKey
	}

	var cmps = getCmp(queryMap)

	q := qb.
		Delete(d.EntityInfo.TableMetaData.Name).
		Where(cmps...).
		Query(d.Session).
		BindMap(queryMap)

	return q.ExecRelease()
}

func (d *DAO) getQueryMap(queryEntity cqlxoEntity.BaseScyllaEntityInterface, columnNames []string) qb.M {
	v := reflect.ValueOf(queryEntity)
	for v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return qb.M{}
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return qb.M{}
	}

	queryMap := qb.M{}
	for _, columnName := range columnNames {
		fieldName, ok := d.EntityInfo.ColumFieldMap[columnName]
		if !ok || fieldName == "" {
			continue
		}
		fieldValue := v.FieldByName(fieldName)
		if !fieldValue.IsValid() {
			continue
		}
		fieldType := fieldValue.Type()
		if !reflect.DeepEqual(fieldValue.Interface(), reflect.Zero(fieldType).Interface()) {
			queryMap[columnName] = fieldValue.Interface()
		}
	}
	return queryMap
}

// Cmp if a filtering comparator that is used in WHERE and IF clauses.
func getCmp(m qb.M) []qb.Cmp {
	var cmps []qb.Cmp
	for k, _ := range m {
		// Eq produces column=?.
		cmps = append(cmps, qb.Eq(k))
	}
	return cmps
}
