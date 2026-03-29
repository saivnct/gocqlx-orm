package cqlxoRepository

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/gocql/gocql"
	"github.com/saivnct/gocqlx-orm/codec"
	"github.com/saivnct/gocqlx-orm/entity"
	"github.com/saivnct/gocqlx-orm/utils/sliceUtils"
	"github.com/scylladb/go-reflectx"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
)

var (
	NoSessionError      = errors.New("no Session Connection Found")
	NoQueryEntityError  = errors.New("no Query Entity")
	InvalidPrimaryKey   = errors.New("invalid PrimaryKey")
	InvalidPartitionKey = errors.New("invalid PartitionKey")
	InvalidTTL          = errors.New("invalid TTL")
)

type BaseScyllaRepository struct {
	EntityInfo cqlxoCodec.EntityInfo
	Session    gocqlx.Session

	BatchConfig *BatchSaveConfig
}

const (
	defaultBatchChunkSize = 50
	defaultBatchType      = gocql.UnloggedBatch
)

type BatchSaveConfig struct {
	ChunkSize int
	Type      gocql.BatchType
}

type QueryOption struct {
	AllowFiltering bool
	Page           []byte
	ItemsPerPage   int
	OrderBy        string
	Order          qb.Order
}

func (d *BaseScyllaRepository) SetBatchSaveConfig(config BatchSaveConfig) {
	d.BatchConfig = &config
}

func (d *BaseScyllaRepository) InitRepository(session gocqlx.Session, m cqlxoEntity.BaseScyllaEntityInterface) error {
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

	//log.Printf("BaseScyllaRepository %s created!", m.TableName())
	return nil
}

func (d *BaseScyllaRepository) checkAndCreateUDT() error {
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

func (d *BaseScyllaRepository) checkAndCreateTable() error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	//log.Println(d.EntityInfo.GetCreateTableStatement())
	err := d.Session.ExecStmt(d.EntityInfo.GetCreateTableStatement())
	return err
}

func (d *BaseScyllaRepository) checkAndCreateIndex() error {
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

func (d *BaseScyllaRepository) Save(entity cqlxoEntity.BaseScyllaEntityInterface) error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	if d.hasTupleColumn() {
		stmt, args, err := d.getInsertStmtAndArgs(entity)
		if err != nil {
			return err
		}

		q := d.Session.Session.Query(stmt, args...)
		defer q.Release()
		return q.Exec()
	}

	q := d.Session.Query(d.EntityInfo.Table.Insert()).BindStruct(entity)
	//log.Printf("Save %s", q.String())
	return q.ExecRelease()
}

// SaveWithTTL inserts one entity with a per-row TTL.
//
// The ttl parameter is expressed in seconds (CQL USING TTL unit), for example:
//   - ttl=60   -> expires in 1 minute
//   - ttl=3600 -> expires in 1 hour
func (d *BaseScyllaRepository) SaveWithTTL(entity cqlxoEntity.BaseScyllaEntityInterface, ttl int64) error {
	if ttl <= 0 {
		return InvalidTTL
	}
	if d.Session.Session == nil {
		return NoSessionError
	}

	stmt := d.getInsertStmtWithTTL()
	args, err := d.getInsertArgs(entity)
	if err != nil {
		return err
	}
	args = append(args, ttl)

	q := d.Session.Session.Query(stmt, args...)
	defer q.Release()
	return q.Exec()
}

func (d *BaseScyllaRepository) SaveMany(entities []cqlxoEntity.BaseScyllaEntityInterface) error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	return d.saveManyInBatches(entities, 0, false)
}

// SaveManyWithTTL inserts multiple entities with the same per-row TTL.
//
// The ttl parameter is expressed in seconds (CQL USING TTL unit), for example:
//   - ttl=60   -> expires in 1 minute
//   - ttl=3600 -> expires in 1 hour
func (d *BaseScyllaRepository) SaveManyWithTTL(entities []cqlxoEntity.BaseScyllaEntityInterface, ttl int64) error {
	if ttl <= 0 {
		return InvalidTTL
	}
	if d.Session.Session == nil {
		return NoSessionError
	}

	return d.saveManyInBatches(entities, ttl, true)
}

func (d *BaseScyllaRepository) hasTupleColumn() bool {
	for _, column := range d.EntityInfo.Columns {
		if column.Type.Type() == gocql.TypeTuple {
			return true
		}
	}
	return false
}

func (d *BaseScyllaRepository) getInsertStmtAndArgs(entity cqlxoEntity.BaseScyllaEntityInterface) (string, []interface{}, error) {
	stmt := d.getInsertStmt()
	args, err := d.getInsertArgs(entity)
	return stmt, args, err
}

func (d *BaseScyllaRepository) getInsertStmt() string {
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

func (d *BaseScyllaRepository) getInsertStmtWithTTL() string {
	return fmt.Sprintf("%s USING TTL ?", d.getInsertStmt())
}

func (d *BaseScyllaRepository) getInsertArgs(entity cqlxoEntity.BaseScyllaEntityInterface) ([]interface{}, error) {
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
			args = append(args, cqlxoCodec.WrapValueForWrite(colInfo.Type, fieldValue, d.mapper()))
			continue
		}

		tupleType := colInfo.Type.(gocql.TupleTypeInfo)
		tupleElems, err := cqlxoCodec.TupleStructToArgs(fieldValue, tupleType, d.mapper())
		if err != nil {
			return nil, fmt.Errorf("tuple column %s: %w", colName, err)
		}
		if len(tupleElems) != len(tupleType.Elems) {
			return nil, fmt.Errorf("tuple column %s: expected %d fields, got %d", colName, len(tupleType.Elems), len(tupleElems))
		}
		args = append(args, tupleElems...)
	}
	return args, nil
}

func (d *BaseScyllaRepository) getColumnInfo(colName string) (cqlxoCodec.ColumnInfo, bool) {
	for _, col := range d.EntityInfo.Columns {
		if col.Name == colName {
			return col, true
		}
	}
	return cqlxoCodec.ColumnInfo{}, false
}

func (d *BaseScyllaRepository) saveManyInBatches(entities []cqlxoEntity.BaseScyllaEntityInterface, ttl int64, useTTL bool) error {
	if len(entities) == 0 {
		return nil
	}

	batchSize := d.getBatchChunkSize()
	batchType := d.getBatchType()

	stmt := d.getInsertStmt()
	if useTTL {
		stmt = d.getInsertStmtWithTTL()
	}

	for start := 0; start < len(entities); start += batchSize {
		end := min(start+batchSize, len(entities))
		batch := d.newBatch(batchType)

		for _, entity := range entities[start:end] {
			args, err := d.getInsertArgs(entity)
			if err != nil {
				return err
			}
			if useTTL {
				args = append(args, ttl)
			}
			batch.Query(stmt, args...)
		}

		if err := d.executeBatch(batch); err != nil {
			return err
		}
	}

	return nil
}

func (d *BaseScyllaRepository) getBatchChunkSize() int {
	if d.BatchConfig != nil && d.BatchConfig.ChunkSize > 0 {
		return d.BatchConfig.ChunkSize
	}
	return defaultBatchChunkSize
}

func (d *BaseScyllaRepository) getBatchType() gocql.BatchType {
	if d.BatchConfig == nil {
		return defaultBatchType
	}

	switch d.BatchConfig.Type {
	case gocql.LoggedBatch, gocql.UnloggedBatch, gocql.CounterBatch:
		return d.BatchConfig.Type
	default:
		return defaultBatchType
	}
}

func (d *BaseScyllaRepository) newBatch(batchType gocql.BatchType) *gocql.Batch {
	return d.Session.Session.Batch(batchType)
}

func (d *BaseScyllaRepository) executeBatch(batch *gocql.Batch) error {
	return d.Session.Session.ExecuteBatch(batch)
}

func (d *BaseScyllaRepository) selectRelease(q *gocqlx.Queryx, result interface{}) error {
	if !d.hasTupleColumn() {
		return q.SelectRelease(result)
	}
	defer q.Release()
	return d.scanTupleIter(q.Iter(), result)
}

func (d *BaseScyllaRepository) scanTupleIter(iter *gocqlx.Iterx, result interface{}) error {
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

	// isPtr is true when the slice holds pointers, e.g. []*Package
	isPtr := elemType.Kind() == reflect.Ptr
	structType := elemType
	for structType.Kind() == reflect.Ptr {
		structType = structType.Elem()
	}

	for scanner.Next() {
		// Always allocate a concrete struct for scanning
		structPtr := reflect.New(structType) // e.g. *Package
		dest, err := d.buildTupleScanDest(structPtr.Elem())
		if err != nil {
			return err
		}
		if err = scanner.Scan(dest...); err != nil {
			return err
		}
		// Append the right type (pointer or value) back to the slice
		if isPtr {
			sliceVal = reflect.Append(sliceVal, structPtr)
		} else {
			sliceVal = reflect.Append(sliceVal, structPtr.Elem())
		}
	}

	if err := scanner.Err(); err != nil {
		return err
	}

	resultVal.Elem().Set(sliceVal)
	return nil
}

func (d *BaseScyllaRepository) buildTupleScanDest(row reflect.Value) ([]interface{}, error) {
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
			dest = append(dest, cqlxoCodec.WrapDestForRead(colInfo.Type, fieldVal, d.mapper()))
			continue
		}

		tupleType := colInfo.Type.(gocql.TupleTypeInfo)
		tupleDest, err := cqlxoCodec.TupleFieldPointers(fieldVal, tupleType, d.mapper())
		if err != nil {
			return nil, fmt.Errorf("tuple column %s: %w", colName, err)
		}
		dest = append(dest, tupleDest...)
	}

	return dest, nil
}

func (d *BaseScyllaRepository) FindAll(result interface{}) error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	q := qb.Select(d.EntityInfo.TableMetaData.Name).Columns(d.EntityInfo.TableMetaData.Columns...).Query(d.Session)
	return d.selectRelease(q, result)
}

func (d *BaseScyllaRepository) mapper() *reflectx.Mapper {
	if d.Session.Mapper != nil {
		return d.Session.Mapper
	}
	return gocqlx.DefaultMapper
}

func (d *BaseScyllaRepository) FindByPrimaryKey(queryEntity cqlxoEntity.BaseScyllaEntityInterface, result interface{}) error {
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

func (d *BaseScyllaRepository) FindByPartitionKey(queryEntity cqlxoEntity.BaseScyllaEntityInterface, result interface{}) error {
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

func (d *BaseScyllaRepository) Find(queryEntity cqlxoEntity.BaseScyllaEntityInterface, allowFiltering bool, result interface{}) error {
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

func (d *BaseScyllaRepository) FindWithOption(queryEntity cqlxoEntity.BaseScyllaEntityInterface, option QueryOption, result interface{}) (nextPage []byte, err error) {
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

func (d *BaseScyllaRepository) CountAll() (int64, error) {
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

func (d *BaseScyllaRepository) Count(queryEntity cqlxoEntity.BaseScyllaEntityInterface, allowFiltering bool) (int64, error) {
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

func (d *BaseScyllaRepository) DeleteAll() error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	return d.Session.ExecStmt(fmt.Sprintf("TRUNCATE %s", d.EntityInfo.TableMetaData.Name))
}

func (d *BaseScyllaRepository) DeleteByPrimaryKey(queryEntity cqlxoEntity.BaseScyllaEntityInterface) error {
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

func (d *BaseScyllaRepository) DeleteByPartitionKey(queryEntity cqlxoEntity.BaseScyllaEntityInterface) error {
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

func (d *BaseScyllaRepository) getQueryMap(queryEntity cqlxoEntity.BaseScyllaEntityInterface, columnNames []string) qb.M {
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
