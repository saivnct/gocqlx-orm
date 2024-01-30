package cqlxoDAO

import (
	"giangbb.studio/go.cqlx.orm/codec"
	"giangbb.studio/go.cqlx.orm/entity"
	"giangbb.studio/go.cqlx.orm/utils/sliceUtils"
	"github.com/scylladb/gocqlx/v2"
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
		err := session.ExecStmt(cqlxoCodec.GetCqlCreateUDTStatement(udt))
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *DAO) CheckAndCreateTable(session gocqlx.Session) error {
	err := session.ExecStmt(d.EntityInfo.GetGreateTableStatement())
	return err
}

//
//func (d *DAO) FindAll(session gocqlx.Session) ([]entity.BaseModelInterface, error) {
//	var rs []entity.BaseModelInterface
//	q := qb.Select(d.TableMetaData.Name).Columns(d.TableMetaData.Columns...).Query(session)
//	err := q.Select(&rs)
//	return rs, err
//}
