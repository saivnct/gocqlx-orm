package test

type CarReward struct {
	Name   string  `db:"name"`
	Cert   string  `db:"cert"`
	Reward float64 `db:"reward"`
}

func (f CarReward) UDTName() string {
	return "car_reward"
}
