package test

type MachineTuple struct {
	Name  string
	Year  int
	Anc   int
	Brand string
}

func (m MachineTuple) Tuple() string { return "" }
