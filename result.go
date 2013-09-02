package monetdb

type Result struct {
	lastInsertId int
	rowsAffected int
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
	return int64(r.lastInsertId), r.err
}

func (r Result) RowsAffected() (int64, error) {
	return int64(r.rowsAffected), r.err
}
