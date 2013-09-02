/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql/driver"
	"fmt"
)

type Conn struct {
	config config
	mapi   *MapiConn
}

func newConn(c config) (*Conn, error) {
	conn := &Conn{
		config: c,
		mapi:   nil,
	}

	m := NewMapi(c.Hostname, c.Port, c.Username, c.Password, c.Database, "sql")
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
	t := newTx(c)

	_, err := c.execute("START TRANSACTION")
	if err != nil {
		t.err = err
	}

	return t, t.err
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
