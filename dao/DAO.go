package cqlxoDAO

import (
	"giangbb.studio/go.cqlx.orm/codec"
	"giangbb.studio/go.cqlx.orm/entity"
	"giangbb.studio/go.cqlx.orm/utils/sliceUtils"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
)

type DAO struct {
	EntityInfo cqlxoCodec.EntityInfo
}

func (d *DAO) InitDAO(session gocqlx.Session, m cqlxoEntity.BaseModelInterface) error {
	entityInfo, err := cqlxoCodec.ParseTableMetaData(m)
	if err != nil {
		return err
	}

	d.EntityInfo = entityInfo

	err = d.CheckAndCreateUDT(session)
	if err != nil {
		return err
	}

	err = d.CheckAndCreateTable(session)
	if err != nil {
		return err
	}

	//log.Printf("DAO %s created!", m.TableName())
	return nil
}

func (d *DAO) CheckAndCreateUDT(session gocqlx.Session) error {
	udts := d.EntityInfo.ScanUDTs()
	udts = sliceUtils.Reverse(udts)

	for _, udt := range udts {
		//log.Println(cqlxoCodec.GetCqlCreateUDTStatement(udt))
		err := session.ExecStmt(cqlxoCodec.GetCqlCreateUDTStatement(udt))
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DAO) CheckAndCreateTable(session gocqlx.Session) error {
	//log.Println(d.EntityInfo.GetGreateTableStatement())
	err := session.ExecStmt(d.EntityInfo.GetGreateTableStatement())
	return err
}

func (d *DAO) Save(session gocqlx.Session, entity cqlxoEntity.BaseModelInterface) error {
	q := session.Query(d.EntityInfo.Table.Insert()).BindStruct(entity)
	//log.Printf("Save %s", q.String())
	return q.ExecRelease()
}

func (d *DAO) FindAll(session gocqlx.Session, result interface{}) error {
	q := qb.Select(d.EntityInfo.TableMetaData.Name).Columns(d.EntityInfo.TableMetaData.Columns...).Query(session)
	err := q.Select(result)
	return err
}
