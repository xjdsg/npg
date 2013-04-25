package core

import (
    "errors"
    "database/sql/driver"
)

type Value struct {
	raw []byte
}

type Field struct {
	Name string
	Type int32
}

type QueryResult struct {
	Fields       []Field
	RowsAffected uint64
	InsertId     uint64
	Rows         [][]Value
}

type Result struct {
	qr    *QueryResult
	index int //current index
}

func NewResult(rowCount, rowsAffected, insertId int64, fields []Field) *Result {
	return &Result{
		qr: &QueryResult{
			Rows:         make([][]Value, int(rowCount)),
			Fields:       fields,
			RowsAffected: uint64(rowsAffected),
			InsertId:     uint64(insertId),
		},
	}
}

func (result *Result) RowsRetrieved() int64 {
	return int64(len(result.qr.Rows))
}


func (result *Result) Rows() [][]Value {
	return result.qr.Rows
}

func (result *Result) Fields() []Field {
	return result.qr.Fields
}

// driver.Result interface
func (result *Result) LastInsertId() (int64, error) {
	return int64(result.qr.InsertId), nil
}

func (result *Result) RowsAffected() (int64, error) {
	return int64(result.qr.RowsAffected), nil
}

// driver.Rows interface
func (result *Result) Columns() []string {
	cols := make([]string, len(result.qr.Fields))
	for i, f := range result.qr.Fields {
		cols[i] = f.Name
	}
	return cols
}

func (result *Result) Close() error {
	result.index = 0
	return nil
}


func (result *Result) Next(dest []driver.Value) error {
	if len(dest) != len(result.qr.Fields) {
        return errors.New("result: field length mismatch")
	}
	if result.index >= len(result.qr.Rows) {
		return errors.New("result: index beyond rows")
	}
	defer func() { result.index++ }()
	for i, v := range result.qr.Rows[result.index] {
		if v.raw != nil {
			dest[i] = convert(int(result.qr.Fields[i].Type), string(v.raw))
		}
	}
	return nil

}
