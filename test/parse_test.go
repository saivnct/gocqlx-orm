package test

import (
	"github.com/davecgh/go-spew/spew"
	"testing"
)

func TestParse(t *testing.T) {
	f := FavoritePlace{
		City:       "Bangkok",
		Country:    "Thailand",
		Population: 10000000,
	}
	spew.Dump(f)

	f.City = "Hanoi"
	spew.Dump(f)
}
