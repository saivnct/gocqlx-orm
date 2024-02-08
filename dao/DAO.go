package cqlxoDAO

import (
	"errors"
	"fmt"
	"giangbb.studio/go.cqlx.orm/codec"
	"giangbb.studio/go.cqlx.orm/entity"
	"giangbb.studio/go.cqlx.orm/utils/sliceUtils"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"reflect"
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

func (d *DAO) InitDAO(session gocqlx.Session, m cqlxoEntity.BaseModelInterface) error {
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

	//log.Println(d.EntityInfo.GetGreateTableStatement())
	err := d.Session.ExecStmt(d.EntityInfo.GetGreateTableStatement())
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

func (d *DAO) Save(entity cqlxoEntity.BaseModelInterface) error {
	if d.Session.Session == nil {
		return NoSessionError
	}
	q := d.Session.Query(d.EntityInfo.Table.Insert()).BindStruct(entity)
	//log.Printf("Save %s", q.String())
	return q.ExecRelease()
}

func (d *DAO) SaveMany(entities []cqlxoEntity.BaseModelInterface) error {
	if d.Session.Session == nil {
		return NoSessionError
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

func (d *DAO) FindAll(result interface{}) error {
	if d.Session.Session == nil {
		return NoSessionError
	}

	q := qb.Select(d.EntityInfo.TableMetaData.Name).Columns(d.EntityInfo.TableMetaData.Columns...).Query(d.Session)
	err := q.SelectRelease(result)
	return err
}

func (d *DAO) FindByPrimaryKey(queryEntity cqlxoEntity.BaseModelInterface, result interface{}) error {
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

	return q.SelectRelease(result)
}

func (d *DAO) FindByPartitionKey(queryEntity cqlxoEntity.BaseModelInterface, result interface{}) error {
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

	return q.SelectRelease(result)
}

func (d *DAO) Find(queryEntity cqlxoEntity.BaseModelInterface, allowFiltering bool, result interface{}) error {
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

	return q.SelectRelease(result)
}

func (d *DAO) FindWithOption(queryEntity cqlxoEntity.BaseModelInterface, option QueryOption, result interface{}) (nextPage []byte, err error) {
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

	return iter.PageState(), iter.Select(result)
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

	return count[0], err
}

func (d *DAO) Count(queryEntity cqlxoEntity.BaseModelInterface, allowFiltering bool) (int64, error) {
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

func (d *DAO) DeleteByPrimaryKey(queryEntity cqlxoEntity.BaseModelInterface) error {
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

func (d *DAO) DeleteByPartitionKey(queryEntity cqlxoEntity.BaseModelInterface) error {
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

func (d *DAO) getQueryMap(queryEntity cqlxoEntity.BaseModelInterface, columnNames []string) qb.M {
	v := reflect.ValueOf(queryEntity)
	queryMap := qb.M{}
	for _, columnName := range columnNames {
		fieldName := d.EntityInfo.ColumFieldMap[columnName]
		fieldValue := v.FieldByName(fieldName)
		fieldType := fieldValue.Type()
		if fieldValue.IsValid() && !reflect.DeepEqual(fieldValue.Interface(), reflect.Zero(fieldType).Interface()) {
			queryMap[columnName] = fieldValue.Interface()
		}
	}
	return queryMap
}

func getCmp(m qb.M) []qb.Cmp {
	var cmps []qb.Cmp
	for k, _ := range m {
		cmps = append(cmps, qb.Eq(k))
	}
	return cmps
}
