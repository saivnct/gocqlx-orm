package test

type LandMark struct {
	City       string   `db:"city"`
	Country    string   `db:"country"`
	Population int64    `db:"population"`
	CheckPoint []string `db:"check_point"`
}

func (f LandMark) UDTName() string {
	return "land_mark"
}
