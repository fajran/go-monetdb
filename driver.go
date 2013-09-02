package monetdb

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strconv"
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
	c, err := parseDSN(name)
	if err != nil {
		return nil, err
	}
	return newConn(c)
}

func parseDSN(name string) (config, error) {
	re := regexp.MustCompile(`((?P<username>.+?)(:(?P<password>.+?))?@)?(?P<hostname>.+?)(:(?P<port>\d+?))?/(?P<database>.+?)`)
	if !re.MatchString(name) {
		return config{}, fmt.Errorf("Invalid DSN")
	}
	m := re.FindAllStringSubmatch(name, -1)[0]
	n := re.SubexpNames()

	c := config{
		Hostname: "localhost",
		Port:     50000,
	}
	for i, v := range m {
		if n[i] == "username" {
			c.Username = v
		} else if n[i] == "password" {
			c.Password = v
		} else if n[i] == "hostname" {
			c.Hostname = v
		} else if n[i] == "port" {
			c.Port, _ = strconv.Atoi(v)
		} else if n[i] == "database" {
			c.Database = v
		}
	}

	return c, nil
}
