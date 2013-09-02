package monetdb

import (
	"strconv"
	"testing"
)

func TestParseDSN(t *testing.T) {
	tcs := [][]string{
		[]string{"me:secret@localhost:1234/testdb", "me", "secret", "localhost", "1234", "testdb"},
		[]string{"me@localhost:1234/testdb", "me", "", "localhost", "1234", "testdb"},
		[]string{"localhost:1234/testdb", "", "", "localhost", "1234", "testdb"},
		[]string{"localhost/testdb", "", "", "localhost", "50000", "testdb"},
		[]string{"localhost"},
		[]string{"/testdb"},
		[]string{"/"},
		[]string{""},
		[]string{":secret@localhost:1234/testdb"},
	}

	for _, tc := range tcs {
		n := tc[0]
		ok := len(tc) > 1
		c, err := parseDSN(n)

		if ok && err != nil {
			t.Errorf("Error parsing DSN: %s -> %v", n, err)
		} else if !ok && err == nil {
			t.Errorf("Error parsing invalid DSN: %s", n)
		}

		if !ok || err != nil {
			continue
		}

		port, _ := strconv.Atoi(tc[4])

		if c.Username != tc[1] {
			t.Errorf("Invalid username: %s, expected: %s", c.Username, tc[1])
		}
		if c.Password != tc[2] {
			t.Errorf("Invalid password: %s, expected: %s", c.Password, tc[2])
		}
		if c.Hostname != tc[3] {
			t.Errorf("Invalid hostname: %s, expected: %s", c.Hostname, tc[3])
		}
		if c.Port != port {
			t.Errorf("Invalid port: %s, expected: %s", c.Port, port)
		}
		if c.Database != tc[5] {
			t.Errorf("Invalid database: %s, expected: %s", c.Database, tc[5])
		}
	}
}
