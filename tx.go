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
