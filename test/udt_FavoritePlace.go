package test

type FavoritePlace struct {
	Place  LandMark `db:"land_mark"`
	Rating int      `db:"rating"`
}

func (f FavoritePlace) UDTName() string {
	return "favorite_place"
}
