package schema

import "strings"

// Quote wraps a MySQL identifier in backticks.
func Quote(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// Qualified quotes database.table.
func Qualified(database, table string) string {
	if database == "" {
		return Quote(table)
	}
	return Quote(database) + "." + Quote(table)
}
