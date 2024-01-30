package test

import "time"

type CitizenIdent struct {
	Id        string
	EndAt     time.Time
	CreatedAt time.Time
	Level     int
}
