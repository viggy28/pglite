package main

import (
	"database/sql"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

func querySqlite(db *sql.DB, query string) (*sql.Rows, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func ExecSqlite(db *sql.DB, query string) (int64, error) {
	result, err := db.Exec(query)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func querySqliteRowDescription(db *sql.DB, query string) (map[string]string, error) {
	rows, err := querySqlite(db, query)
	if err != nil {
		return nil, err
	}
	// get column names
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	// NOTE: This is assuming that the column names and column types are returned in the same order
	// If they aren't in the same order then we have to query the catalog table such as sqlite_master
	// SELECT * FROM sqlite_master WHERE type='table' and name='$tableName';
	output := make(map[string]string, len(cols))
	for index, col := range cols {
		output[col] = columnTypes[index].DatabaseTypeName()
	}
	return output, nil
}

// prepareDataRow returns a result set for a given query
func prepareDataRow(db *sql.DB, query string) ([][][]byte, error) {
	rows, err := querySqlite(db, query)
	if err != nil {
		return nil, err
	}
	// get column names
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// since we don't know the number of rows without actually scanning it
	// create an output slice of byte which represents result set
	var output [][][]byte

	for rows.Next() {
		// refer on how to dynamically get sql output https://betterprogramming.pub/dynamic-sql-query-with-go-8aeedaa02907
		// row is the storage for row.Scans.
		// for printing, []byte is sufficient for most of the data type,
		// including NULLABLE column, which will be empty
		row := make([][]byte, len(cols))

		log.Println("row:", len(row))
		// through rowPtr, we can scan data to row.
		// without rowPtr, we need to do very complex type assertion
		// and dereference when printing (e.g. *(rowPtr[i].(*[]byte)))
		// rowPtr... expands rowPtr slice to individual arguments

		// row.Scan accepts variadic POINTER (of any type) arguments.
		// to make it able to take dynamic number of arguments,
		// we need a []any variable and use it in ellipsis operator.
		// rowPtr is just a slice of pointers pointing to every element in row
		rowPtr := make([]any, len(cols))
		for i := range row {
			rowPtr[i] = &row[i]
		}

		err := rows.Scan(rowPtr...)

		if err != nil {
			return nil, err
		}
		output = append(output, row)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return output, err
}
