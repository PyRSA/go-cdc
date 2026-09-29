package mysql

import (
	"strings"

	"github.com/PyRSA/go-cdc/common/schema"
)

// ChunkKeySQL is the boundary query. A nil low reads from the start of the key order.
func ChunkKeySQL(database, table, key string, low *string, limit int) (string, []any) {
	statement := "SELECT " + schema.Quote(key) + " FROM " + schema.Qualified(database, table)
	var args []any
	if low != nil {
		statement += " WHERE " + schema.Quote(key) + " >= ?"
		args = append(args, *low)
	}
	statement += " ORDER BY " + schema.Quote(key) + " LIMIT ?"
	args = append(args, limit)
	return statement, args
}

// ReadChunkSQL reads one page of a snapshot range.
// after is the last chunk key already returned; the next page starts strictly after it.
// A nil high means there is no upper bound. limit 0 reads the whole range.
func ReadChunkSQL(database, table string, columns []string, key, low string, high *string, after any, limit int) (string, []any) {
	selectList := "*"
	if len(columns) > 0 {
		quoted := make([]string, len(columns))
		for i, column := range columns {
			quoted[i] = schema.Quote(column)
		}
		selectList = strings.Join(quoted, ", ")
	}
	statement := "SELECT " + selectList + " FROM " + schema.Qualified(database, table)
	var where []string
	var args []any
	if low != "" {
		where = append(where, schema.Quote(key)+" >= ?")
		args = append(args, low)
	}
	if high != nil {
		where = append(where, schema.Quote(key)+" < ?")
		args = append(args, *high)
	}
	if after != nil {
		where = append(where, schema.Quote(key)+" > ?")
		args = append(args, after)
	}
	if len(where) > 0 {
		statement += " WHERE " + strings.Join(where, " AND ")
	}
	statement += " ORDER BY " + schema.Quote(key)
	if limit > 0 {
		statement += " LIMIT ?"
		args = append(args, limit)
	}
	return statement, args
}
