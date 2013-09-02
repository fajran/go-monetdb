package monetdb

import (
	"database/sql"
	"database/sql/driver"
)

func init() {
	sql.Register("monetdb", &Driver{})
}

type Driver struct {
}

type config struct {
	Username string
	Password string
	Hostname string
	Database string
	Port     int
}

func (*Driver) Open(name string) (driver.Conn, error) {
	c := config{
		Username: "voc",
		Password: "voc",
		Hostname: "localhost",
		Database: "voc",
		Port:     50000,
	}

	return newConn(c)
}
