package cqlxoDAO

import (
	"errors"
	"giangbb.studio/go.cqlx.orm/codec"
	"giangbb.studio/go.cqlx.orm/entity"
	"giangbb.studio/go.cqlx.orm/utils/sliceUtils"
	"github.com/gocql/gocql"
)

var (
	NoSessionErr = errors.New("no session connected")
)

type DAO struct {
	EntityInfo cqlxoCodec.EntityInfo
	session    *gocql.Session
}

func (d *DAO) InitDAO(session *gocql.Session, m cqlxoEntity.BaseModelInterface) error {
	entityInfo, err := cqlxoCodec.ParseTableMetaData(m)
	if err != nil {
		return err
	}

	d.EntityInfo = entityInfo
	d.session = session

	err = d.CheckAndCreateUDT()
	if err != nil {
		return err
	}

	err = d.CheckAndCreateTable()
	if err != nil {
		return err
	}

	//log.Printf("DAO %s created!", m.TableName())
	return nil
}

func (d *DAO) CheckAndCreateUDT() error {
	if d.session == nil {
		return NoSessionErr
	}

	udts := d.EntityInfo.ScanUDTs()
	udts = sliceUtils.Reverse(udts)

	for _, udt := range udts {
		err := d.session.Query(cqlxoCodec.GetCqlCreateUDTStatement(udt)).Exec()
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DAO) CheckAndCreateTable() error {
	if d.session == nil {
		return NoSessionErr
	}

	err := d.session.Query(d.EntityInfo.GetGreateTableStatement()).Exec()
	return err
}

//
//func (d *DAO) Save(session gocqlx.Session, entity cqlxoEntity.BaseModelInterface) error {
//	q := session.Query(d.EntityInfo.Table.Insert()).BindStruct(entity)
//	log.Printf("Save %s", q.String())
//	return q.ExecRelease()
//}
//
//func (d *DAO) FindAll(session gocqlx.Session, result interface{}) error {
//	q := qb.Select(d.EntityInfo.TableMetaData.Name).Columns(d.EntityInfo.TableMetaData.Columns...).Query(session)
//	err := q.Select(result)
//	return err
//}
