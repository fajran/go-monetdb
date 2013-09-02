package monetdb

import (
	"database/sql/driver"
	"fmt"

	"monetdb/mapi"
)

type Conn struct {
	config config
	mapi   *mapi.Conn
}

func newConn(c config) (*Conn, error) {
	conn := &Conn{
		config: c,
		mapi:   nil,
	}

	m := mapi.New(c.Hostname, c.Port, c.Username, c.Password, c.Database, "sql")
	err := m.Connect()
	if err != nil {
		return conn, err
	}

	conn.mapi = m
	return conn, nil
}

func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return newStmt(c, query), nil
}

func (c *Conn) Close() error {
	c.mapi.Disconnect()
	c.mapi = nil
	return nil
}

func (c *Conn) Begin() (driver.Tx, error) {
	// TODO
	return Tx{}, nil
}

func (c *Conn) cmd(cmd string) (string, error) {
	if c.mapi == nil {
		return "", fmt.Errorf("connection closed")
	}

	return c.mapi.Cmd(cmd)
}

func (c *Conn) execute(q string) (string, error) {
	cmd := fmt.Sprintf("s%s;", q)
	return c.cmd(cmd)
}
