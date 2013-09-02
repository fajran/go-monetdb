/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

package monetdb

import (
	"database/sql/driver"
	"fmt"
	"io"
)

type Rows struct {
	stmt   *Stmt
	active bool

	queryId int

	err error

	rowNum      int
	offset      int
	lastRowId   int
	rowCount    int
	rows        [][]driver.Value
	description []description
	columns     []string
}

func newRows(s *Stmt) *Rows {
	return &Rows{
		stmt:   s,
		active: true,
		err:    nil,

		columns: nil,
		rowNum:  0,
	}
}

func (r *Rows) Columns() []string {
	if r.columns == nil {
		r.columns = make([]string, len(r.description))
		for i, d := range r.description {
			r.columns[i] = d.columnName
		}
	}
	return r.columns
}

func (r *Rows) Close() error {
	r.active = false
	return nil
}

var cnt = 0

func (r *Rows) Next(dest []driver.Value) error {
	if !r.active {
		return fmt.Errorf("Rows closed")
	}
	if r.queryId == -1 {
		return fmt.Errorf("Query didn't result in a resultset")
	}

	if r.rowNum >= r.rowCount {
		return io.EOF
	}

	if r.rowNum >= r.offset+len(r.rows) {
		err := r.fetchNext()
		if err != nil {
			return err
		}
	}

	for i, v := range r.rows[r.rowNum-r.offset] {
		if vv, ok := v.(string); ok {
			dest[i] = []byte(vv)
		} else {
			dest[i] = v
		}
	}
	r.rowNum += 1

	return nil
}

const (
	ARRAY_SIZE = 100
)

func min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (r *Rows) fetchNext() error {
	if r.rowNum >= r.rowCount {
		return io.EOF
	}

	r.offset += len(r.rows)
	end := min(r.rowCount, r.rowNum+ARRAY_SIZE)
	amount := end - r.offset

	cmd := fmt.Sprintf("Xexport %d %d %d", r.queryId, r.offset, amount)
	res, err := r.stmt.conn.cmd(cmd)
	if err != nil {
		return err
	}

	r.stmt.storeResult(res)
	r.rows = r.stmt.rows
	r.description = r.stmt.description

	return nil
}
