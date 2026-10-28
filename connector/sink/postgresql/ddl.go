package postgresql

import (
	"context"
	"errors"
	"fmt"
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
	if value, ok := columnDefault(column); ok {
		parts = append(parts, "DEFAULT", value)
	}
	return strings.Join(parts, " "), nil
}

// columnDefault renders the source default when PostgreSQL can hold it. A
// default this sink cannot prove portable is left out instead of being guessed:
// the sink writes an explicit value for every projected column, so a missing
// default never changes what a captured row stores.
func columnDefault(column schema.Column) (string, bool) {
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
	if upper == "CURRENT_TIMESTAMP" || strings.HasPrefix(upper, "CURRENT_TIMESTAMP(") {
		return value, true
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
	if isNumericLiteral(value) {
		return value, true
	}
	return "'" + strings.ReplaceAll(value, "'", "''") + "'", true
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
	_, err := strconv.ParseFloat(value, 64)
	return err == nil
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
		_ = beginTx.Rollback(context.WithoutCancel(opCtx))
		return execErr
	}
	if err := beginTx.Commit(opCtx); err != nil {
		_ = beginTx.Rollback(context.WithoutCancel(opCtx))
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
// unique index or a constraint is not: the conflict target every upsert uses
// depends on those, and reproducing them wrongly would silently change which
// rows the target considers the same, so the job stops instead.
func (s *Sink) applyIndexChange(target Target, change schema.Change) error {
	if secondaryIndexChange(change.DDL) {
		s.logger().Warn("postgresql: secondary index change skipped; PostgreSQL keeps its own index plan",
			"table", target.String(), "ddl", change.DDL)
		return nil
	}
	return fmt.Errorf("sink.postgresql: index change on %s touches a constraint or unique key the sink cannot reproduce; apply it by hand (ddl %q)",
		target, change.DDL)
}

// secondaryIndexChange reports whether the statement only creates or drops a
// non-unique secondary index. Anything that names a key or a constraint, and
// anything the check cannot recognise, is not a plain index change.
func secondaryIndexChange(ddl string) bool {
	upper := " " + strings.Join(strings.Fields(strings.ToUpper(ddl)), " ") + " "
	for _, keyword := range []string{"PRIMARY", "UNIQUE", "FOREIGN", "CHECK", "CONSTRAINT", "EXCLUDE"} {
		if strings.Contains(upper, " "+keyword+" ") || strings.Contains(upper, " "+keyword+"(") {
			return false
		}
	}
	for _, phrase := range []string{
		"CREATE INDEX", "CREATE FULLTEXT INDEX", "CREATE SPATIAL INDEX", "DROP INDEX",
		"ADD INDEX", "ADD KEY", "ADD FULLTEXT", "ADD SPATIAL", "DROP KEY",
	} {
		if strings.Contains(upper, " "+phrase+" ") {
			return true
		}
	}
	return false
}
