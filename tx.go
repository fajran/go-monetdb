/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

type Tx struct {
	conn *Conn
	err  error
}

func newTx(c *Conn) *Tx {
	return &Tx{
		conn: c,
		err:  nil,
	}
}

func (t *Tx) Commit() error {
	_, err := t.conn.execute("COMMIT")
	return err
}

func (t *Tx) Rollback() error {
	_, err := t.conn.execute("ROLLBACK")
	return err
}
