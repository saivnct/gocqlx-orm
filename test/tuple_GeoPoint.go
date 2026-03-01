package test

// GeoPoint is a tuple that contains a UDT element (Address).
// Tuple elements are ordered struct fields.
type GeoPoint struct {
	Lat  float64 `db:"lat"`
	Lng  float64 `db:"lng"`
	Info Address `db:"info"`
}

func (GeoPoint) Tuple() string {
	return "geo_point"
}
