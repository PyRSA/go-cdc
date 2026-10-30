package postgresql

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/PyRSA/go-cdc/common/schema"
)

// CreateTable makes the target table ready for the source definition. With
// sink.auto-create-table on it creates the table from the source schema, never
// from the first row; with it off the table must already exist.
//
// An existing table is always validated against the live catalogue. Its name is
// not proof that it can hold these rows: a missing column, a different type, or
// a key the conflict target cannot use has to surface before the first batch,
// not as a rejected write halfway through it.
func (s *Sink) CreateTable(table schema.Table) error {
	target, err := s.changeTarget(schema.Change{Database: table.Database, Table: table.Table})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(s.writeContext())
	defer cancel()
	state, exists, err := s.meta.refresh(ctx, target)
	if err != nil {
		return err
	}
	if exists {
		return validateTargetTable(table, state)
	}
	if !s.cfg.AutoCreate {
		return fmt.Errorf("sink.postgresql: table %s does not exist: sink.auto-create-table is false, so the target must be created by hand", target)
	}
	if len(table.Columns) == 0 {
		return fmt.Errorf("sink.postgresql: cannot create %s: the source definition has no columns", target)
	}
	statement, err := createTableSQL(target, table)
	if err != nil {
		return err
	}
	s.warnUnreproducedAttributes(target, table)
	if err := s.ensureSchema(ctx, target.Schema); err != nil {
		return err
	}
	if err := s.execDDL(ctx, statement); err != nil {
		// A concurrent writer may have created the same table between the read
		// and the CREATE; the read-back below decides, exactly as it does for a
		// table this process created.
		if !isDuplicateRelation(err) {
			return err
		}
		s.logger().Info("postgresql: target table was created concurrently", "table", target.String())
	}
	if err := s.verifyCreated(ctx, target, table); err != nil {
		return err
	}
	s.logger().Info("postgresql: target table created",
		"schema", target.Schema, "table", target.Table, "columns", len(table.Columns))
	return nil
}

// warnUnreproducedAttributes records the source attributes a created table does
// not carry. PostgreSQL has no AUTO_INCREMENT, and the sink writes the value the
// source sent for every projected column, so the generator is left out rather
// than replaced by a sequence that could hand out a value the source never had.
func (s *Sink) warnUnreproducedAttributes(target Target, table schema.Table) {
	var columns []string
	for _, column := range table.Columns {
		if column.AutoIncrement {
			columns = append(columns, column.Name)
		}
	}
	if len(columns) == 0 {
		return
	}
	s.logger().Warn("postgresql: auto increment is not reproduced on the created table; the sink always writes the source value",
		"table", target.String(), "columns", strings.Join(columns, ", "))
}

// verifyCreated re-reads the target after CREATE TABLE and validates it as a
// whole. The statement that ran is not the evidence; the catalogue is.
func (s *Sink) verifyCreated(ctx context.Context, target Target, table schema.Table) error {
	state, exists, err := s.meta.refresh(ctx, target)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("sink.postgresql: table %s does not exist after CREATE TABLE", target)
	}
	return validateTargetTable(table, state)
}

// ensureSchema proves the target schema exists, and creates it when
// auto-create-table authorises creating the table it belongs to.
func (s *Sink) ensureSchema(ctx context.Context, name string) error {
	exists, err := s.meta.schemaExists(ctx, name)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	if err := s.execDDL(ctx, "CREATE SCHEMA IF NOT EXISTS "+quoteIdentifier(name)); err != nil {
		return err
	}
	s.logger().Info("postgresql: target schema created", "schema", name)
	return nil
}

// createTableSQL renders the CREATE TABLE statement of one projected source
// table. Primary keys and unique constraints are created because the DML's
// ON CONFLICT needs a matching conflict target; a plain secondary index only
// changes how PostgreSQL plans a query and is left to the operator.
func createTableSQL(target Target, table schema.Table) (string, error) {
	definitions := make([]string, 0, len(table.Columns)+1+len(table.Indexes))
	for _, column := range table.Columns {
		clause, err := columnClause(target, column)
		if err != nil {
			return "", err
		}
		definitions = append(definitions, clause)
	}
	if len(table.PrimaryKeys) > 0 {
		definitions = append(definitions, "PRIMARY KEY ("+quoteIdentifiers(table.PrimaryKeys)+")")
	}
	for _, index := range table.Indexes {
		columns, ok := uniqueIndexColumns(index)
		if !ok {
			continue
		}
		definitions = append(definitions, "UNIQUE ("+quoteIdentifiers(columns)+")")
	}
	return "CREATE TABLE " + target.quote() + " (" + strings.Join(definitions, ", ") + ")", nil
}

// uniqueIndexColumns reports the columns of a unique index this sink can turn
// into a unique constraint. A prefix index, a fulltext or spatial index, and a
// non-unique index have no equivalent that the conflict target could use.
func uniqueIndexColumns(index schema.Index) ([]string, bool) {
	if !index.Unique || len(index.Columns) == 0 {
		return nil, false
	}
	switch strings.ToUpper(index.Type) {
	case "", "BTREE", "HASH":
	default:
		return nil, false
	}
	for _, prefix := range index.Prefixes {
		if prefix > 0 {
			return nil, false
		}
	}
	return index.Columns, true
}

// columnClause renders one column definition. PostgreSQL has no AUTO_INCREMENT,
// and the sink always writes the value the source sent, so an auto-increment
// column is created as its plain integer type without a value generator.
func columnClause(target Target, column schema.Column) (string, error) {
	pgType, err := mapColumnType(column.Type)
	if err != nil {
		return "", fmt.Errorf("sink.postgresql: table %s column %s: %w", target, column.Name, err)
	}
	parts := []string{quoteIdentifier(column.Name), pgType}
	if !column.Nullable {
		parts = append(parts, "NOT NULL")
	}
	if value, ok := columnDefault(column, pgType); ok {
		parts = append(parts, "DEFAULT", value)
	}
	return strings.Join(parts, " "), nil
}

// columnDefault renders the source default when PostgreSQL can hold it. A
// default this sink cannot prove portable is left out instead of being guessed:
// the sink writes an explicit value for every projected column, so a missing
// default never changes what a captured row stores.
//
// pgType decides how a value is spelled, because the source default is text for
// every column type. '0' is the number 0 in an integer column and the
// one-character string "0" in a char column, and PostgreSQL refuses the wrong
// spelling outright (42804 on a numeric literal written to a character column),
// so the target type, not the shape of the text, decides.
//
// The same text can also be a literal or an expression: information_schema
// spells the string default 'CURRENT_TIMESTAMP' and the function default
// CURRENT_TIMESTAMP identically, so the target type separates them again. See
// timestampDefault.
func columnDefault(column schema.Column, pgType string) (string, bool) {
	if !column.HasDefault {
		return "", false
	}
	value := strings.TrimSpace(column.Default)
	if value == "" {
		// An explicit empty-string default must stay an empty string.
		return "''", true
	}
	upper := strings.ToUpper(value)
	if upper == "NULL" {
		return "NULL", true
	}
	if expression, ok := timestampDefault(value, pgType); ok {
		return expression, true
	}
	if column.DefaultExpr {
		// information_schema reports generated defaults as expressions, which are
		// MySQL-specific and have no verified PostgreSQL equivalent.
		return "", false
	}
	if strings.HasPrefix(value, "(") {
		return "", false
	}
	if isZeroDate(value) {
		// PostgreSQL cannot hold the MySQL zero date, so copying the default
		// would fail the CREATE; the value itself already fails every write.
		return "", false
	}
	if acceptsNumericLiteral(pgType) && isNumericLiteral(value) {
		return value, true
	}
	return "'" + strings.ReplaceAll(value, "'", "''") + "'", true
}

// timestampDefault renders CURRENT_TIMESTAMP and CURRENT_TIMESTAMP(n) as the
// PostgreSQL expression, and only on a timestamp column.
//
// information_schema spells two different defaults the same way: the function on
// a temporal column, and the literal string 'CURRENT_TIMESTAMP' on a character
// column. Only the target type tells them apart. PostgreSQL accepts
// CURRENT_TIMESTAMP as a character column default through an I/O cast, so
// emitting the expression there would store the moment of the INSERT where the
// source stores the literal, and nothing would fail. A timestamp column cannot
// hold that literal at all — MySQL refuses the default — so on a timestamp
// target the text is the expression.
//
// The argument has to be a fractional-seconds precision PostgreSQL accepts. Any
// other spelling, and every emittable expression this function does not prove,
// is left to the caller: the DefaultExpr branch drops it, a literal is quoted.
func timestampDefault(value, pgType string) (string, bool) {
	if parseIntType(pgType).name != "timestamp" {
		return "", false
	}
	upper := strings.ToUpper(strings.TrimSpace(value))
	if upper == "CURRENT_TIMESTAMP" {
		return "CURRENT_TIMESTAMP", true
	}
	const prefix = "CURRENT_TIMESTAMP("
	if !strings.HasPrefix(upper, prefix) || !strings.HasSuffix(upper, ")") {
		return "", false
	}
	precision, err := strconv.Atoi(strings.TrimSpace(upper[len(prefix) : len(upper)-1]))
	if err != nil || precision < 0 || precision > maxTimePrecision {
		return "", false
	}
	return "CURRENT_TIMESTAMP(" + strconv.Itoa(precision) + ")", true
}

// acceptsNumericLiteral reports whether an unquoted numeric literal is a valid
// default for the column type. Only the numeric and floating point families
// are: a character, temporal, binary, or json column takes the same text as a
// string literal, where the quotes are part of the value's meaning.
func acceptsNumericLiteral(pgType string) bool {
	switch parseIntType(pgType).name {
	case "smallint", "integer", "bigint", "numeric", "real", "double precision":
		return true
	}
	return false
}

// isZeroDate reports the MySQL zero date and zero datetime sentinels.
func isZeroDate(value string) bool {
	switch {
	case strings.HasPrefix(value, "0000-00-00"):
		return true
	case strings.HasPrefix(value, "0000-00-00 00:00:00"):
		return true
	}
	return false
}

func isNumericLiteral(value string) bool {
	if value == "" {
		return false
	}
	number, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return false
	}
	// NaN and the infinities have no unquoted spelling in SQL. Quoted, they are
	// the values numeric and double precision accept.
	return !math.IsNaN(number) && !math.IsInf(number, 0)
}

// execDDL runs one statement in its own transaction.
func (s *Sink) execDDL(ctx context.Context, statement string) error {
	return s.execDDLAll(ctx, []string{statement})
}

// execDDLAll runs the statements of one schema change in a single transaction,
// so a change that renames and retypes a column applies whole or not at all. A
// failed statement rolls the transaction back and is reported with the
// statement text and the server error.
func (s *Sink) execDDLAll(ctx context.Context, statements []string) error {
	if len(statements) == 0 {
		return nil
	}
	p := s.currentPool()
	if p == nil {
		return fmt.Errorf("sink.postgresql: schema change after close")
	}
	opCtx, cancel := context.WithTimeout(ctx, s.opTimeout)
	defer cancel()
	beginTx, err := p.Begin(opCtx)
	if err != nil {
		return fmt.Errorf("sink.postgresql: begin schema change transaction: %w", err)
	}
	batch := &pgx.Batch{}
	for _, statement := range statements {
		batch.Queue(statement)
	}
	results := beginTx.SendBatch(opCtx, batch)
	var execErr error
	for i := range statements {
		if _, err := results.Exec(); err != nil && execErr == nil {
			execErr = fmt.Errorf("sink.postgresql: schema change failed: %s: %w", statements[i], err)
		}
	}
	if closeErr := results.Close(); closeErr != nil && execErr == nil {
		execErr = fmt.Errorf("sink.postgresql: close schema change batch: %w", closeErr)
	}
	if execErr != nil {
		// The same bounded rollback the DML path uses: the operation deadline may
		// already have fired, and an unbounded rollback would wait forever on a
		// connection that stopped answering.
		rollback(beginTx, opCtx)
		return execErr
	}
	if err := beginTx.Commit(opCtx); err != nil {
		rollback(beginTx, opCtx)
		return fmt.Errorf("sink.postgresql: commit schema change: %w", err)
	}
	return nil
}

// isDuplicateRelation reports the SQLSTATE of a CREATE that lost a race.
func isDuplicateRelation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P07"
}

// applyIndexChange decides one index statement. A plain secondary index only
// affects query planning on the target, so it is skipped with a warning. A
// unique index, a constraint, or a statement that mixes an index with another
// alteration is not: the conflict target every upsert uses depends on those, and
// skipping the rest would leave the target behind the source with nothing to
// show for it, so the job stops instead.
func (s *Sink) applyIndexChange(target Target, change schema.Change) error {
	if secondaryIndexChange(change.DDL) {
		s.logger().Warn("postgresql: secondary index change skipped; PostgreSQL keeps its own index plan",
			"table", target.String(), "ddl", change.DDL)
		return nil
	}
	return fmt.Errorf("sink.postgresql: change on %s is not a plain secondary index change: it names a key or a constraint, or it mixes an index with another alteration the sink cannot reproduce; apply it by hand (ddl %q)",
		target, change.DDL)
}

// secondaryIndexChange reports whether every clause of the statement only
// creates or drops a non-unique secondary index. Anything that names a key or a
// constraint, mixes in another alteration, or cannot be read at all is not a
// plain index change.
//
// One recognised phrase somewhere in the text is not enough. MySQL accepts a
// compound ALTER TABLE, and the classifier names a statement that contains an
// index clause an index change even when the same statement adds or drops a
// column. The whole statement reaches this check, so answering true for it would
// skip that column change. Every clause is checked instead.
func secondaryIndexChange(ddl string) bool {
	clauses, ok := indexStatementClauses(ddl)
	if !ok || len(clauses) == 0 {
		return false
	}
	for _, clause := range clauses {
		if !secondaryIndexClause(clause) {
			return false
		}
	}
	return true
}

// indexStatementClauses returns the clauses of one statement for the check
// above: an ALTER TABLE is split on the commas that separate its alterations,
// and any other statement is one clause. String literals, quoted identifiers,
// and comments are masked first, so their text can neither end a clause early
// nor be read as a keyword. A statement whose text cannot be masked whole is
// reported as unreadable instead of being split on a guess.
func indexStatementClauses(ddl string) ([]string, bool) {
	masked, ok := maskInsignificant(strings.ToUpper(ddl))
	if !ok {
		return nil, false
	}
	text := strings.Join(strings.Fields(masked), " ")
	if text == "" {
		return nil, false
	}
	if !startsWithKeyword(text, "ALTER TABLE") {
		return []string{text}, true
	}
	body, ok := afterTableReference(text)
	if !ok || body == "" {
		return nil, false
	}
	return splitTopLevel(body), true
}

// maskedQuotedName stands in for one quoted identifier, string literal, or
// comment. It is not an identifier byte and not a space, so a masked name keeps
// the word boundaries around it: ADD INDEX `idx` still starts with the two words
// the check looks for.
const maskedQuotedName = '*'

// maskInsignificant replaces every string literal, quoted identifier, and
// comment, leaving everything else in place: a quoted span becomes one
// placeholder followed by spaces, and a comment becomes spaces. Not one byte of
// the masked span is left to be read as syntax.
//
// A name such as `add index`, a default such as 'ADD INDEX', and a note such as
// /* UNIQUE */ are text, not syntax: reading them as the statement's own words
// would either skip a change that must run or stop a change that is a plain
// index. A comment also holds commas, which would otherwise split a clause.
//
// ok is false when a literal or a block comment has no closing delimiter. The
// scan then cannot tell where a clause ends — a stray quote would swallow the
// comma before the next clause — so the statement is reported as unreadable
// rather than masked up to a boundary it cannot prove.
func maskInsignificant(text string) (string, bool) {
	out := []byte(text)
	for i := 0; i < len(out); i++ {
		switch c := out[i]; {
		case c == '\'' || c == '"' || c == '`':
			end, closed := quotedEnd(out, i)
			maskSpan(out, i, end, true)
			if !closed {
				return "", false
			}
			i = end
		case c == '/' && i+1 < len(out) && out[i+1] == '*':
			end, closed := blockCommentEnd(out, i)
			maskSpan(out, i, end, false)
			if !closed {
				return "", false
			}
			i = end
		case c == '-' && i+1 < len(out) && out[i+1] == '-':
			end := i
			for end < len(out) && out[end] != '\n' {
				end++
			}
			maskSpan(out, i, end, false)
			i = end
		}
	}
	return string(out), true
}

// maskSpan replaces text[from:to] with spaces, keeping the span as one
// placeholder token when the caller asks for one, so the words around it stay
// separated and the span itself never reads as a keyword.
func maskSpan(text []byte, from, to int, placeholder bool) {
	if to > len(text) {
		to = len(text)
	}
	for i := from; i < to; i++ {
		text[i] = ' '
	}
	if placeholder && from < to {
		text[from] = maskedQuotedName
	}
}

// quotedEnd returns the index just past the closing delimiter of the quoted span
// that starts at open, and whether that delimiter was found. A doubled delimiter
// is an escaped one; the backslash escapes MySQL also accepts are not modelled,
// which leaves an odd quote dangling and reports the statement as unreadable.
func quotedEnd(text []byte, open int) (int, bool) {
	delimiter := text[open]
	for i := open + 1; i < len(text); i++ {
		if text[i] != delimiter {
			continue
		}
		if i+1 < len(text) && text[i+1] == delimiter {
			i++
			continue
		}
		return i + 1, true
	}
	return len(text), false
}

// blockCommentEnd returns the index just past the */ that closes the comment
// starting at open, and whether the comment is closed at all.
func blockCommentEnd(text []byte, open int) (int, bool) {
	for i := open + 2; i+1 < len(text); i++ {
		if text[i] == '*' && text[i+1] == '/' {
			return i + 2, true
		}
	}
	return len(text), false
}

// afterTableReference returns the alteration list that follows the table name of
// an ALTER TABLE statement. The name may be quoted and may be schema-qualified.
func afterTableReference(text string) (string, bool) {
	rest := strings.TrimSpace(text[len("ALTER TABLE"):])
	for {
		width, ok := tableNameWidth(rest)
		if !ok {
			return "", false
		}
		rest = strings.TrimSpace(rest[width:])
		if !strings.HasPrefix(rest, ".") {
			return rest, true
		}
		rest = strings.TrimSpace(rest[1:])
	}
}

// tableNameWidth returns the length of the table name at the start of text. A
// quoted name has already been reduced to one placeholder byte.
func tableNameWidth(text string) (int, bool) {
	if text == "" {
		return 0, false
	}
	if text[0] == maskedQuotedName {
		return 1, true
	}
	i := 0
	for i < len(text) && isIdentifierByte(text[i]) {
		i++
	}
	return i, i > 0
}

// splitTopLevel splits one alteration list on the commas that separate its
// clauses. Text inside parentheses stays together, so the column list of an
// index is never read as several clauses.
func splitTopLevel(text string) []string {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(text); i++ {
		switch text[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(text[start:i]))
				start = i + 1
			}
		}
	}
	return append(out, strings.TrimSpace(text[start:]))
}

// secondaryIndexClause reports whether one clause only creates or drops a
// non-unique secondary index.
func secondaryIndexClause(clause string) bool {
	if clause == "" {
		return false
	}
	for _, keyword := range []string{"PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "CONSTRAINT", "EXCLUDE"} {
		if hasKeyword(clause, keyword) {
			return false
		}
	}
	for _, phrase := range []string{
		"ADD INDEX", "ADD KEY", "ADD FULLTEXT", "ADD SPATIAL", "DROP KEY",
		"CREATE FULLTEXT INDEX", "CREATE SPATIAL INDEX", "CREATE INDEX", "DROP INDEX",
	} {
		if startsWithKeyword(clause, phrase) {
			return true
		}
	}
	return false
}

// startsWithKeyword reports whether text begins with keyword as a bare word, so
// a column named INDEXED is not the INDEX keyword.
func startsWithKeyword(text, keyword string) bool {
	if len(text) < len(keyword) || !strings.EqualFold(text[:len(keyword)], keyword) {
		return false
	}
	return len(text) == len(keyword) || !isIdentifierByte(text[len(keyword)])
}
