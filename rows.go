package monetdb

import (
	"database/sql/driver"
	"fmt"
	"io"
)

type Rows struct {
	stmt   Stmt
	active bool

	queryId int64

	err error

	data *RowsData
}

type RowsData struct {
	rowNum      int64
	offset      int64
	lastRowId   int64
	rowCount    int64
	rows        [][]driver.Value
	description []description
	columns     []string
}

func newRows(s Stmt) Rows {
	return Rows{
		stmt:   s,
		active: true,
		err:    nil,
		data: &RowsData{
			columns: nil,
			rowNum:  0,
		},
	}
}

func (r Rows) Columns() []string {
	if r.data.columns == nil {
		r.data.columns = make([]string, len(r.data.description))
		for i, d := range r.data.description {
			r.data.columns[i] = d.columnName
		}
	}
	return r.data.columns
}

func (r Rows) Close() error {
	r.active = false
	r.data = nil
	return nil
}

var cnt = 0

func (r Rows) Next(dest []driver.Value) error {
	if !r.active {
		return fmt.Errorf("rows closed")
	}
	if r.queryId == -1 {
		return fmt.Errorf("query didn't result in a resultset")
	}

	if r.data.rowNum >= r.data.rowCount {
		return io.EOF
	}

	if r.data.rowNum >= r.data.offset+int64(len(r.data.rows)) {
		err := r.fetchNext()
		if err != nil {
			return err
		}
	}

	fmt.Printf("rows len: %d\n", len(r.data.rows))
	for i, v := range r.data.rows[r.data.rowNum-r.data.offset] {
		if vv, ok := v.(string); ok {
			dest[i] = []byte(vv)
		} else {
			dest[i] = v
		}
	}
	r.data.rowNum += 1

	return nil
}

const (
	ARRAY_SIZE = 100
)

func min(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func (r Rows) fetchNext() error {
	if r.data.rowNum >= r.data.rowCount {
		return io.EOF
	}

	r.data.offset += int64(len(r.data.rows))
	end := min(r.data.rowCount, r.data.rowNum+ARRAY_SIZE)
	amount := end - r.data.offset

	cmd := fmt.Sprintf("Xexport %d %d %d", r.queryId, r.data.offset, amount)
	res, err := r.stmt.conn.cmd(cmd)
	if err != nil {
		return err
	}

	r.stmt.storeResult(res)
	r.data.rows = r.stmt.data.rows
	r.data.description = r.stmt.data.description

	return nil
}
