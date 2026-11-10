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
		// Table identity is filled before classifyAlter so an unrecognized
		// ALTER (ActionOther) still names the target. Callers can refuse to
		// skip that statement for a captured table instead of checkpointing past it.
		change.Database, change.Table = tableAfter(text, "ALTER TABLE")
		classifyAlter(&change, text)
	}
	return change
}

// alterKind is one top-level ALTER TABLE clause category the classifier knows.
type alterKind int

const (
	alterNone alterKind = iota
	alterAddColumn
	alterDropColumn
	alterRenameColumn
	alterModifyColumn
	alterIndex
	alterRenameTable
	alterUnsupported
)

// classifyAlter classifies one ALTER TABLE by its top-level clauses. Mixed
// supported kinds, or any unsupported clause beside a supported one, become
// ActionOther so callers refuse the statement instead of applying only half.
func classifyAlter(change *Change, text string) {
	rest := afterTableRef(text, "ALTER TABLE")
	var kinds []alterKind
	var firstAdd, firstRename, firstModify, firstDrop string
	var firstRenameNew string
	for _, raw := range splitClauses(rest) {
		clause := strings.TrimSpace(raw)
		if clause == "" || isAlterOptionClause(clause) {
			continue
		}
		kind := classifyAlterClause(clause)
		switch kind {
		case alterNone:
			continue
		case alterUnsupported:
			change.Action = ActionOther
			return
		case alterAddColumn:
			if firstAdd == "" {
				firstAdd = identAfter(clause, "ADD COLUMN", "ADD")
			}
		case alterDropColumn:
			if firstDrop == "" {
				firstDrop = identAfter(clause, "DROP COLUMN")
			}
		case alterRenameColumn:
			if firstRename == "" {
				firstRename, firstRenameNew = twoIdentsAfter(clause, "RENAME COLUMN", "CHANGE COLUMN", "CHANGE")
			}
		case alterModifyColumn:
			if firstModify == "" {
				firstModify = identAfter(clause, "MODIFY COLUMN", "MODIFY")
			}
		}
		if !containsAlterKind(kinds, kind) {
			kinds = append(kinds, kind)
		}
	}
	if len(kinds) != 1 {
		change.Action = ActionOther
		return
	}
	switch kinds[0] {
	case alterAddColumn:
		change.Action = ActionAddColumn
		change.Column = firstAdd
	case alterDropColumn:
		change.Action = ActionDropColumn
		change.Column = firstDrop
	case alterRenameColumn:
		change.Action = ActionRenameColumn
		change.Column, change.NewName = firstRename, firstRenameNew
	case alterModifyColumn:
		change.Action = ActionModifyColumn
		change.Column = firstModify
	case alterIndex:
		change.Action = ActionIndex
	case alterRenameTable:
		change.Action = ActionRenameTable
	default:
		change.Action = ActionOther
	}
}

func containsAlterKind(kinds []alterKind, want alterKind) bool {
	for _, kind := range kinds {
		if kind == want {
			return true
		}
	}
	return false
}

// isAlterOptionClause reports ALGORITHM=/LOCK= execution options, which do not
// change schema shape and must not force ActionOther on an otherwise pure ALTER.
func isAlterOptionClause(clause string) bool {
	return startsWithWord(clause, "ALGORITHM") || startsWithWord(clause, "LOCK")
}

func classifyAlterClause(clause string) alterKind {
	switch {
	case startsWithWord(clause, "ADD"):
		body := strings.TrimSpace(clause[len("ADD"):])
		if startsWithWord(body, "COLUMN") {
			return alterAddColumn
		}
		switch addClauseKind(body) {
		case alterIndex:
			return alterIndex
		case alterUnsupported:
			return alterUnsupported
		default:
			return alterAddColumn
		}
	case startsWithWord(clause, "DROP"):
		body := strings.TrimSpace(clause[len("DROP"):])
		if startsWithWord(body, "COLUMN") {
			return alterDropColumn
		}
		if startsWithWord(body, "INDEX") || startsWithWord(body, "KEY") || startsWithWord(body, "PRIMARY") {
			return alterIndex
		}
		return alterUnsupported
	case startsWithWord(clause, "RENAME"):
		body := strings.TrimSpace(clause[len("RENAME"):])
		if startsWithWord(body, "COLUMN") {
			return alterRenameColumn
		}
		if startsWithWord(body, "TO") || startsWithWord(body, "AS") {
			return alterRenameTable
		}
		return alterUnsupported
	case startsWithWord(clause, "CHANGE"):
		return alterRenameColumn
	case startsWithWord(clause, "MODIFY"):
		return alterModifyColumn
	default:
		return alterUnsupported
	}
}

// addClauseKind distinguishes index-like ADD clauses from column ADD and from
// unsupported ADD CONSTRAINT / FOREIGN / CHECK / PARTITION forms.
func addClauseKind(body string) alterKind {
	for _, keyword := range []string{"INDEX", "KEY", "PRIMARY", "UNIQUE", "FULLTEXT", "SPATIAL"} {
		if startsWithWord(body, keyword) {
			return alterIndex
		}
	}
	for _, keyword := range []string{"CONSTRAINT", "FOREIGN", "CHECK", "PARTITION"} {
		if startsWithWord(body, keyword) {
			return alterUnsupported
		}
	}
	return alterAddColumn
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

// AddColumns returns the column names of every ADD clause in one ALTER TABLE
// statement, in statement order. Index-only ADD clauses (INDEX/KEY/PRIMARY/
// UNIQUE/FULLTEXT/SPATIAL/CONSTRAINT/FOREIGN/CHECK/PARTITION) are skipped.
// An empty result means the statement adds no column.
func AddColumns(sql string) []string {
	text := stripComments(strings.TrimSpace(sql))
	if !strings.HasPrefix(strings.ToUpper(text), "ALTER TABLE") {
		return nil
	}
	var out []string
	for _, clause := range splitClauses(afterTableRef(text, "ALTER TABLE")) {
		clause = strings.TrimSpace(clause)
		if !startsWithWord(clause, "ADD") {
			continue
		}
		body := strings.TrimSpace(clause[len("ADD"):])
		if startsWithWord(body, "COLUMN") {
			body = strings.TrimSpace(body[len("COLUMN"):])
		}
		if isIndexClause(body) {
			continue
		}
		if name, _ := readIdent(body); name != "" {
			out = append(out, name)
		}
	}
	return out
}

func isIndexClause(body string) bool {
	for _, keyword := range []string{"INDEX", "KEY", "PRIMARY", "UNIQUE", "FULLTEXT", "SPATIAL", "CONSTRAINT", "FOREIGN", "CHECK", "PARTITION"} {
		if startsWithWord(body, keyword) {
			return true
		}
	}
	return false
}

// startsWithWord reports whether s begins with word followed by a non-identifier
// byte, so a column named KEYED is not mistaken for the KEY keyword.
func startsWithWord(s, word string) bool {
	if len(s) < len(word) || !strings.EqualFold(s[:len(word)], word) {
		return false
	}
	if len(s) == len(word) {
		return true
	}
	next := rune(s[len(word)])
	return !unicode.IsLetter(next) && !unicode.IsDigit(next) && next != '_' && next != '$'
}

// splitClauses splits on commas outside parentheses, strings, and identifiers.
func splitClauses(s string) []string {
	var out []string
	depth, start := 0, 0
	inSingle, inDouble, inBack := false, false, false
	for i := 0; i < len(s); i++ {
		switch s[i] {
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
		case '(':
			if !inSingle && !inDouble && !inBack {
				depth++
			}
		case ')':
			if !inSingle && !inDouble && !inBack && depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 && !inSingle && !inDouble && !inBack {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
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
