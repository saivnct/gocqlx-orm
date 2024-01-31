package test

import cqlxoEntity "giangbb.studio/go.cqlx.orm/entity"

type MachineTuple struct {
	cqlxoEntity.Tuple
	Name  string
	Year  int
	Anc   int
	Brand string
}
