package monetdb

type Result struct {
	lastInsertId int64
	rowsAffected int64
	err          error
}

func newResult() Result {
	return Result{
		lastInsertId: 0,
		rowsAffected: 0,
		err:          nil,
	}
}

func (r Result) LastInsertId() (int64, error) {
	return r.lastInsertId, r.err
}

func (r Result) RowsAffected() (int64, error) {
	return r.rowsAffected, r.err
}
