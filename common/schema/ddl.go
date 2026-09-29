package schema

import (
	"strings"
	"unicode"
)

// Classify parses one MySQL DDL statement into an action and a table identity.
// Column-level ALTER statements also return the affected column.
func Classify(sql string) Change {
	raw := strings.TrimSpace(sql)
	text := stripComments(raw)
	upper := strings.ToUpper(text)
	change := Change{Action: ActionOther, DDL: raw}

	switch {
	case strings.HasPrefix(upper, "CREATE TABLE"):
		change.Action = ActionCreate
		change.Database, change.Table = tableAfter(text, "CREATE TABLE")
	case strings.HasPrefix(upper, "DROP TABLE"):
		change.Action = ActionDropTable
		change.Database, change.Table = tableAfter(text, "DROP TABLE")
	case strings.HasPrefix(upper, "TRUNCATE TABLE") || strings.HasPrefix(upper, "TRUNCATE "):
		change.Action = ActionTruncate
		kw := "TRUNCATE TABLE"
		if !strings.HasPrefix(upper, "TRUNCATE TABLE") {
			kw = "TRUNCATE"
		}
		change.Database, change.Table = tableAfter(text, kw)
	case strings.HasPrefix(upper, "RENAME TABLE"):
		change.Action = ActionRenameTable
		change.Database, change.Table = tableAfter(text, "RENAME TABLE")
	case strings.HasPrefix(upper, "CREATE UNIQUE INDEX"), strings.HasPrefix(upper, "CREATE INDEX"), strings.HasPrefix(upper, "DROP INDEX"):
		change.Action = ActionIndex
	case strings.HasPrefix(upper, "ALTER TABLE"):
		change.Database, change.Table = tableAfter(text, "ALTER TABLE")
		classifyAlter(&change, text)
	}
	return change
}

func classifyAlter(change *Change, text string) {
	rest := afterTableRef(text, "ALTER TABLE")
	upper := strings.ToUpper(rest)
	switch {
	case hasWord(upper, "ADD COLUMN"), hasWord(upper, "ADD "):
		if hasWord(upper, "ADD INDEX") || hasWord(upper, "ADD KEY") || hasWord(upper, "ADD PRIMARY") || hasWord(upper, "ADD UNIQUE") || hasWord(upper, "ADD FULLTEXT") || hasWord(upper, "ADD SPATIAL") {
			change.Action = ActionIndex
			return
		}
		change.Action = ActionAddColumn
		change.Column = identAfter(rest, "ADD COLUMN", "ADD")
	case hasWord(upper, "DROP COLUMN"):
		change.Action = ActionDropColumn
		change.Column = identAfter(rest, "DROP COLUMN")
	case hasWord(upper, "DROP INDEX"), hasWord(upper, "DROP KEY"), hasWord(upper, "DROP PRIMARY"):
		change.Action = ActionIndex
	case hasWord(upper, "RENAME COLUMN"):
		change.Action = ActionRenameColumn
		change.Column, change.NewName = twoIdentsAfter(rest, "RENAME COLUMN")
	case hasWord(upper, "CHANGE COLUMN"), hasWord(upper, "CHANGE "):
		change.Action = ActionRenameColumn
		change.Column, change.NewName = twoIdentsAfter(rest, "CHANGE COLUMN", "CHANGE")
	case hasWord(upper, "MODIFY COLUMN"), hasWord(upper, "MODIFY "):
		change.Action = ActionModifyColumn
		change.Column = identAfter(rest, "MODIFY COLUMN", "MODIFY")
	case hasWord(upper, "RENAME TO"), hasWord(upper, "RENAME AS"):
		change.Action = ActionRenameTable
	default:
		change.Action = ActionOther
	}
}

func hasWord(upper, phrase string) bool {
	return strings.Contains(upper, phrase)
}

func stripComments(sql string) string {
	var b strings.Builder
	inSingle, inDouble, inBack := false, false, false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if !inSingle && !inDouble && !inBack && c == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			end := strings.Index(sql[i+2:], "*/")
			if end < 0 {
				break
			}
			i += 2 + end + 1
			b.WriteByte(' ')
			continue
		}
		if !inSingle && !inDouble && !inBack && c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			if nl := strings.IndexByte(sql[i:], '\n'); nl >= 0 {
				i += nl
			} else {
				break
			}
			b.WriteByte(' ')
			continue
		}
		switch c {
		case '\'':
			if !inDouble && !inBack {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle && !inBack {
				inDouble = !inDouble
			}
		case '`':
			if !inSingle && !inDouble {
				inBack = !inBack
			}
		}
		b.WriteByte(c)
	}
	return strings.Join(strings.Fields(b.String()), " ")
}

func tableAfter(sql, keyword string) (string, string) {
	rest := afterKeyword(sql, keyword)
	if strings.HasPrefix(strings.ToUpper(rest), "IF NOT EXISTS ") {
		rest = rest[len("IF NOT EXISTS "):]
	}
	if strings.HasPrefix(strings.ToUpper(rest), "IF EXISTS ") {
		rest = rest[len("IF EXISTS "):]
	}
	db, table, _ := readQualified(rest)
	return db, table
}

func afterTableRef(sql, keyword string) string {
	rest := afterKeyword(sql, keyword)
	_, _, n := readQualified(rest)
	rest = strings.TrimSpace(rest[n:])
	return rest
}

func afterKeyword(sql, keyword string) string {
	upper := strings.ToUpper(sql)
	kw := strings.ToUpper(keyword)
	i := strings.Index(upper, kw)
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(sql[i+len(keyword):])
}

func identAfter(sql string, keywords ...string) string {
	upper := strings.ToUpper(sql)
	best := -1
	kwLen := 0
	for _, kw := range keywords {
		i := strings.Index(upper, strings.ToUpper(kw))
		if i >= 0 && (best < 0 || i < best) {
			best = i
			kwLen = len(kw)
		}
	}
	if best < 0 {
		return ""
	}
	name, _ := readIdent(strings.TrimSpace(sql[best+kwLen:]))
	return name
}

func twoIdentsAfter(sql string, keywords ...string) (string, string) {
	upper := strings.ToUpper(sql)
	best := -1
	kwLen := 0
	for _, kw := range keywords {
		i := strings.Index(upper, strings.ToUpper(kw))
		if i >= 0 && (best < 0 || i < best) {
			best = i
			kwLen = len(kw)
		}
	}
	if best < 0 {
		return "", ""
	}
	rest := strings.TrimSpace(sql[best+kwLen:])
	oldName, n := readIdent(rest)
	rest = strings.TrimSpace(rest[n:])
	if strings.HasPrefix(strings.ToUpper(rest), "TO ") {
		rest = strings.TrimSpace(rest[len("TO "):])
	}
	newName, _ := readIdent(rest)
	return oldName, newName
}

func readQualified(s string) (db, table string, consumed int) {
	first, n := readIdent(s)
	if n == 0 {
		return "", "", 0
	}
	rest := s[n:]
	if strings.HasPrefix(rest, ".") {
		second, n2 := readIdent(rest[1:])
		return first, second, n + 1 + n2
	}
	return "", first, n
}

func readIdent(s string) (string, int) {
	if s == "" {
		return "", 0
	}
	if s[0] == '`' {
		end := strings.IndexByte(s[1:], '`')
		if end < 0 {
			return strings.Trim(s, "`"), len(s)
		}
		return strings.ReplaceAll(s[1:1+end], "``", "`"), end + 2
	}
	i := 0
	for i < len(s) && (unicode.IsLetter(rune(s[i])) || unicode.IsDigit(rune(s[i])) || s[i] == '_' || s[i] == '$') {
		i++
	}
	return s[:i], i
}
