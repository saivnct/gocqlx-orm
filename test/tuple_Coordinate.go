package test

type Coordinate struct {
	Lat float64 `db:"lat"`
	Lng float64 `db:"lng"`
}

func (Coordinate) Tuple() string {
	return "coordinate"
}
