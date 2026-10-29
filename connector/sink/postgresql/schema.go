package postgresql

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/PyRSA/go-cdc/common/schema"
)

// Schema change kinds, spelled the way sink.schema.change.include accepts them.
// composer/definition validates the configured list against this vocabulary;
// the two lists are kept equal by a test in this package.
const (
	changeAddColumn    = "add.column"
	changeDropColumn   = "drop.column"
	changeRenameColumn = "rename.column"
	changeModifyColumn = "modify.column"
	changeDropTable    = "drop.table"
	changeTruncate     = "truncate"
)

// SupportedChangeKinds returns the change names this sink can apply. It is
// exported so the configuration parser can reject an unknown include entry
// without keeping a second copy of the vocabulary.
func SupportedChangeKinds() []string {
	return []string{changeAddColumn, changeDropColumn, changeRenameColumn, changeModifyColumn, changeDropTable, changeTruncate}
}

// changeKind names one action in the configuration vocabulary.
func changeKind(action schema.Action) (string, bool) {
	switch action {
	case schema.ActionAddColumn:
		return changeAddColumn, true
	case schema.ActionDropColumn:
		return changeDropColumn, true
	case schema.ActionRenameColumn:
		return changeRenameColumn, true
	case schema.ActionModifyColumn:
		return changeModifyColumn, true
	case schema.ActionDropTable:
		return changeDropTable, true
	case schema.ActionTruncate:
		return changeTruncate, true
	default:
		return "", false
	}
}

// destructive reports whether a change removes data that no DML statement can
// put back, which is what the drop-truncate switch guards.
func destructive(kind string) bool {
	return kind == changeDropTable || kind == changeTruncate
}

// ApplySchema applies one schema change to the routed target.
//
// The runner routes the statement and hands it here only for the kinds this
// sink declares; the gates below make the operator's configuration the final
// authority. A refused or failed change stops the job instead of being skipped:
// a target that keeps the old structure, or keeps rows the source deleted,
// diverges with nothing to show for it.
//
// Every successful change ends by re-reading the target from pg_catalog before
// the metadata and statement caches are reused, because the statement that ran
// is not proof that the table now has the shape the source asked for.
func (s *Sink) ApplySchema(change schema.Change) error {
	target, err := s.changeTarget(change)
	if err != nil {
		return err
	}
	// DDL runs on the lifecycle context, not on the caller's: a table discovered
	// while the signal is being handled must still be altered before the drain.
	ctx, cancel := context.WithCancel(s.writeContext())
	defer cancel()
	switch change.Action {
	case schema.ActionIndex:
		// An index is not a column change. It is classified here so a plain
		// secondary index is skipped and a key-carrying one stops the job.
		return s.applyIndexChange(target, change)
	case schema.ActionAddColumn, schema.ActionDropColumn, schema.ActionRenameColumn,
		schema.ActionModifyColumn, schema.ActionDropTable, schema.ActionTruncate:
	default:
		// RENAME TABLE and every statement the classifier could not name: the
		// target table is part of the checkpoint's table identity, so moving it
		// without a verified rewrite would leave checkpoint and target disagreeing.
		return fmt.Errorf("sink.postgresql: schema change %s on %s is not supported by this sink; apply it by hand (ddl %q)", change.Action, target, change.DDL)
	}
	kind, ok := changeKind(change.Action)
	if !ok {
		return fmt.Errorf("sink.postgresql: schema change %s on %s is not supported", change.Action, target)
	}
	if err := s.checkChangePolicy(kind, change); err != nil {
		return err
	}
	switch change.Action {
	case schema.ActionAddColumn:
		err = s.addColumn(ctx, target, change)
	case schema.ActionDropColumn:
		err = s.dropColumn(ctx, target, change)
	case schema.ActionRenameColumn, schema.ActionModifyColumn:
		err = s.alterColumn(ctx, target, change)
	case schema.ActionDropTable:
		err = s.dropTable(ctx, target)
	case schema.ActionTruncate:
		err = s.truncate(ctx, target)
	}
	if err != nil {
		return err
	}
	// The statement text of a target depends on its column names and keys, so a
	// change invalidates the templates rendered for it.
	s.builder.Forget(target)
	return nil
}

// checkChangePolicy applies sink.schema.change.enabled, the include list and
// the drop-truncate switch. The include list and the switch are both required
// for a destructive change: a config that lists truncate but leaves the switch
// off still refuses it.
func (s *Sink) checkChangePolicy(kind string, change schema.Change) error {
	name := change.Database
	if name == "" {
		// The change carries no schema because no route named one; sink.schema
		// applies, and the refusal must name the table the operator sees.
		name = s.cfg.Schema
	}
	name += "." + change.Table
	if !s.cfg.SchemaChange {
		return fmt.Errorf("sink.postgresql: %s on %s refused: schema changes are disabled (sink.schema.change.enabled: false)", kind, name)
	}
	if len(s.cfg.SchemaChangeInclude) > 0 && !containsFold(s.cfg.SchemaChangeInclude, kind) {
		return fmt.Errorf("sink.postgresql: %s on %s refused: sink.schema.change.include does not list %q", kind, name, kind)
	}
	if destructive(kind) && !s.cfg.DropTruncate {
		return fmt.Errorf("sink.postgresql: %s on %s refused: dropping and truncating are disabled (sink.schema.change.drop-truncate.enabled: false)", kind, name)
	}
	return nil
}

// changeTarget resolves the PostgreSQL table of one change. The runner already
// resolved a matched route into Database; an empty Database means no route
// named a schema, so the configured sink.schema applies, exactly as it does for
// DML. A source database name never becomes a schema by accident.
func (s *Sink) changeTarget(change schema.Change) (Target, error) {
	if change.Table == "" {
		return Target{}, fmt.Errorf("sink.postgresql: %s arrived without a table name (ddl %q)", change.Action, change.DDL)
	}
	if strings.Contains(change.Table, ".") {
		return Target{}, fmt.Errorf("sink.postgresql: sink-table %s must be exactly schema.table", change.Table)
	}
	name := change.Database
	if name == "" {
		name = s.cfg.Schema
	}
	if name == "" {
		return Target{}, fmt.Errorf("sink.postgresql: %s on table %s has no schema configured", change.Action, change.Table)
	}
	return Target{Schema: name, Table: change.Table}, nil
}

// addColumn adds every column the statement names. A single ADD COLUMN carries
// the structured ColumnDef the source read at capture time; a statement that
// adds several columns carries their names only, so their definitions are read
// from the source definition the catalogue holds now. Nothing is reconstructed
// from DDL text, because the text is not a reliable source for type,
// nullability, or defaults (and two different engines disagree on it).
func (s *Sink) addColumn(ctx context.Context, target Target, change schema.Change) error {
	names := change.AddColumns
	if len(names) == 0 && change.Column != "" {
		names = []string{change.Column}
	}
	if len(names) == 0 {
		return fmt.Errorf("sink.postgresql: ADD COLUMN on %s arrived without a column name (ddl %q)", target, change.DDL)
	}
	columns, err := s.addedColumns(ctx, change, names)
	if err != nil {
		return err
	}
	statements := make([]string, 0, len(columns))
	for _, column := range columns {
		if !column.Nullable && !column.HasDefault {
			// MySQL fills the rows it already has with an implicit value; PostgreSQL
			// has none, so the statement would either fail or store a value the
			// source never had. An AUTO_INCREMENT column is refused for the same
			// reason: adding it does not generate values for the rows already there.
			return fmt.Errorf("sink.postgresql: ADD COLUMN %q on %s is NOT NULL without a default, which PostgreSQL cannot apply to a table that already holds rows; add it with a default or make it nullable first", column.Name, target)
		}
		clause, err := columnClause(target, column)
		if err != nil {
			return err
		}
		statements = append(statements, "ALTER TABLE "+target.quote()+" ADD COLUMN IF NOT EXISTS "+clause)
	}
	if err := s.execDDLAll(ctx, statements); err != nil {
		return err
	}
	// A column that already existed with another definition makes IF NOT EXISTS
	// a no-op, so the read-back is what decides whether the change took effect.
	for _, column := range columns {
		if _, err := s.verifyAddedColumn(ctx, target, column); err != nil {
			return err
		}
	}
	return nil
}

// addedColumns returns the structured definition of every added column.
func (s *Sink) addedColumns(ctx context.Context, change schema.Change, names []string) ([]schema.Column, error) {
	if len(names) == 1 && change.ColumnDef != nil {
		column := *change.ColumnDef
		if !strings.EqualFold(column.Name, names[0]) {
			return nil, fmt.Errorf("sink.postgresql: ADD COLUMN on %s.%s names %q but describes %q", change.Database, change.Table, names[0], column.Name)
		}
		return []schema.Column{column}, nil
	}
	described, err := s.currentSourceDefinition(ctx, change)
	if err != nil {
		return nil, err
	}
	out := make([]schema.Column, 0, len(names))
	for _, name := range names {
		column, ok := sourceColumnFold(described.Columns, name)
		if !ok {
			return nil, fmt.Errorf("sink.postgresql: column %q is not in the current definition of the source table %s.%s; a change the source no longer shows cannot be replayed safely",
				name, change.SourceDatabase, change.SourceTable)
		}
		out = append(out, column)
	}
	return out, nil
}

// dropColumn removes one column. It refuses to drop a column that carries a key
// or references another table: the first would silently remove the conflict
// target every upsert relies on, the second is a dependency PostgreSQL would
// reject halfway through the statement.
func (s *Sink) dropColumn(ctx context.Context, target Target, change schema.Change) error {
	name := change.Column
	if name == "" {
		return fmt.Errorf("sink.postgresql: DROP COLUMN on %s arrived without a column name (ddl %q)", target, change.DDL)
	}
	state, exists, err := s.meta.refresh(ctx, target)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("sink.postgresql: cannot drop column %q: table %s does not exist", name, target)
	}
	live, ok := state.columnFold(name)
	if !ok {
		// A replayed statement finds the column gone and has nothing left to do.
		return nil
	}
	name = live.Name
	dependents, err := s.meta.dependents(ctx, target, name)
	if err != nil {
		return err
	}
	if blocking := blockingDependents(dependents); len(blocking) > 0 {
		return fmt.Errorf("sink.postgresql: cannot drop column %q from %s: %s uses it; dropping the column would take the constraint that the conflict target needs with it",
			name, target, strings.Join(blocking, ", "))
	}
	if err := s.execDDL(ctx, "ALTER TABLE "+target.quote()+" DROP COLUMN IF EXISTS "+quoteIdentifier(name)); err != nil {
		return err
	}
	return s.verifyColumnAbsent(ctx, target, name)
}

// alterColumn applies RENAME COLUMN, CHANGE COLUMN and MODIFY COLUMN.
//
// MySQL's CHANGE COLUMN restates the whole column definition, so it can rename
// and retype at once; treating it as a pure rename would leave the type behind.
// The new definition is read from the current source catalogue, which is the
// only structured description available, and the statement is refused when that
// read cannot prove what the change was.
func (s *Sink) alterColumn(ctx context.Context, target Target, change schema.Change) error {
	oldName := change.Column
	if oldName == "" {
		return fmt.Errorf("sink.postgresql: %s on %s arrived without a column name (ddl %q)", change.Action, target, change.DDL)
	}
	newName := change.NewName
	if newName == "" {
		newName = oldName
	}
	state, exists, err := s.meta.refresh(ctx, target)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("sink.postgresql: cannot alter column %q: table %s does not exist", oldName, target)
	}
	_, hasOld := state.columnFold(oldName)
	_, hasNew := state.columnFold(newName)
	switch {
	case oldName == newName:
		if !hasOld {
			return fmt.Errorf("sink.postgresql: column %q is not in %s; the source change cannot be applied to a column the target does not have", oldName, target)
		}
	case hasOld && hasNew:
		return fmt.Errorf("sink.postgresql: cannot rename %q to %q: %s already has both columns", oldName, newName, target)
	case !hasOld && hasNew:
		// A replayed statement: the rename already happened. Only a restated
		// definition can still have work left, and that is checked below.
	case !hasOld && !hasNew:
		return fmt.Errorf("sink.postgresql: neither %q nor %q is a column of %s", oldName, newName, target)
	}

	var want *schema.Column
	if restatesColumnDefinition(change.DDL) {
		column, err := s.sourceColumnDefinition(ctx, change, newName)
		if err != nil {
			return err
		}
		want = &column
	}

	// MySQL keeps the case a column was written with, so the target may hold the
	// name in a different case than the statement names it. Both the rename and
	// the retype have to use the spelling the target actually holds: a quoted
	// name with the wrong case matches no column.
	live, liveOK := state.columnFold(oldName)
	if !liveOK {
		// A replay, or a change that names the new column: the column carries the
		// new name already.
		live, liveOK = state.columnFold(newName)
	}
	targetName := newName
	switch {
	case oldName != newName && hasOld:
		// The rename below makes the column carry exactly newName.
	case liveOK:
		targetName = live.Name
	}

	var statements []string
	if oldName != newName && hasOld {
		statements = append(statements, "ALTER TABLE "+target.quote()+
			" RENAME COLUMN "+quoteIdentifier(live.Name)+" TO "+quoteIdentifier(newName))
	}
	if want != nil {
		if !liveOK {
			return fmt.Errorf("sink.postgresql: column %q is not in %s", newName, target)
		}
		adjustments, err := columnAdjustments(target, live, targetName, *want)
		if err != nil {
			return err
		}
		statements = append(statements, adjustments...)
	}
	if len(statements) > 0 {
		// One transaction: a change that renames and retypes is applied whole or
		// not at all, so a failure cannot leave the column under its new name
		// with the old type.
		if err := s.execDDLAll(ctx, statements); err != nil {
			return err
		}
	}
	if want != nil {
		_, err := s.verifyColumn(ctx, target, *want)
		return err
	}
	_, _, err = s.meta.refresh(ctx, target)
	return err
}

// columnAdjustments renders the type and nullability statements that make the
// live column match the source definition. A type that is not a verified
// lossless widening is refused here, before any statement runs.
func columnAdjustments(target Target, live columnState, name string, want schema.Column) ([]string, error) {
	wantType, err := mapColumnType(want.Type)
	if err != nil {
		return nil, fmt.Errorf("sink.postgresql: table %s column %s: %w", target, name, err)
	}
	var out []string
	if !samePGType(wantType, live.Type) {
		if err := safeAlterType(live.Type, wantType); err != nil {
			return nil, fmt.Errorf("sink.postgresql: column %s of %s: %w", name, target, err)
		}
		out = append(out, "ALTER TABLE "+target.quote()+" ALTER COLUMN "+quoteIdentifier(name)+" TYPE "+wantType)
	}
	switch {
	case want.Nullable && live.NotNull:
		out = append(out, "ALTER TABLE "+target.quote()+" ALTER COLUMN "+quoteIdentifier(name)+" DROP NOT NULL")
	case !want.Nullable && !live.NotNull:
		// Tightening is applied and left to PostgreSQL to validate: the server
		// refuses it when a row already holds NULL, which is the only safe way
		// to find out.
		out = append(out, "ALTER TABLE "+target.quote()+" ALTER COLUMN "+quoteIdentifier(name)+" SET NOT NULL")
	}
	return out, nil
}

// sourceColumnDefinition reads one column of the current source definition.
//
// The description is the source's state now, not the state the binlog
// statement ran against. That is enough because the sink only ever moves a
// column forward to what the source holds, and a source column that the
// statement named but the source no longer has cannot be interpreted at all:
// the change is refused instead of guessed.
func (s *Sink) sourceColumnDefinition(ctx context.Context, change schema.Change, name string) (schema.Column, error) {
	described, err := s.currentSourceDefinition(ctx, change)
	if err != nil {
		return schema.Column{}, err
	}
	column, ok := sourceColumnFold(described.Columns, name)
	if !ok {
		return schema.Column{}, fmt.Errorf("sink.postgresql: column %q is not in the current definition of the source table %s.%s; a change the source no longer shows cannot be replayed safely",
			name, change.SourceDatabase, change.SourceTable)
	}
	return column, nil
}

// currentSourceDefinition reads the source table the change came from.
func (s *Sink) currentSourceDefinition(ctx context.Context, change schema.Change) (schema.Table, error) {
	database, table := change.SourceDatabase, change.SourceTable
	if database == "" || table == "" {
		return schema.Table{}, fmt.Errorf("sink.postgresql: %s on %s.%s arrived without the source table it came from", change.Action, change.Database, change.Table)
	}
	described, err := s.DescribeTable(ctx, database, table)
	if err != nil {
		return schema.Table{}, fmt.Errorf("sink.postgresql: cannot prove %s on %s.%s: %w", change.Action, change.Database, change.Table, err)
	}
	return described, nil
}

// dropTable removes the target table. The switch that authorises it was checked
// before this runs.
func (s *Sink) dropTable(ctx context.Context, target Target) error {
	if err := s.execDDL(ctx, "DROP TABLE IF EXISTS "+target.quote()); err != nil {
		return err
	}
	// Nothing is read back: the structure that would be cached belongs to a
	// table that no longer exists.
	s.meta.forget(target)
	return nil
}

// truncate empties the target table. An absent table is an error rather than a
// silent success: TRUNCATE carries no IF EXISTS, and a pipeline whose target is
// missing is out of sync in a way the operator has to see.
func (s *Sink) truncate(ctx context.Context, target Target) error {
	_, exists, err := s.meta.refresh(ctx, target)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("sink.postgresql: cannot truncate %s: the table does not exist", target)
	}
	return s.execDDL(ctx, "TRUNCATE TABLE "+target.quote())
}

// verifyColumn re-reads the target and proves one column has the definition the
// source asks for. The statement that ran is not the evidence; the catalogue is.
func (s *Sink) verifyColumn(ctx context.Context, target Target, column schema.Column) (tableState, error) {
	wantType, err := mapColumnType(column.Type)
	if err != nil {
		return tableState{}, fmt.Errorf("sink.postgresql: table %s column %s: %w", target, column.Name, err)
	}
	state, exists, err := s.meta.refresh(ctx, target)
	if err != nil {
		return tableState{}, err
	}
	if !exists {
		return tableState{}, fmt.Errorf("sink.postgresql: table %s does not exist after the schema change", target)
	}
	live, ok := state.columnFold(column.Name)
	if !ok {
		return tableState{}, fmt.Errorf("sink.postgresql: column %q is missing from %s after the schema change", column.Name, target)
	}
	if !samePGType(wantType, live.Type) {
		return tableState{}, fmt.Errorf("sink.postgresql: column %q of %s is %s after the schema change, the source %s maps to %s",
			column.Name, target, live.Type, column.Type, wantType)
	}
	if live.NotNull && column.Nullable {
		return tableState{}, fmt.Errorf("sink.postgresql: column %q of %s is NOT NULL after the schema change while the source column is nullable",
			column.Name, target)
	}
	if !live.NotNull && !column.Nullable {
		return tableState{}, fmt.Errorf("sink.postgresql: column %q of %s is nullable after the schema change while the source column is NOT NULL",
			column.Name, target)
	}
	return state, nil
}

// verifyAddedColumn re-reads the target and proves one added column can hold
// every value the source sends. It is the read-back of addColumn, and it
// relaxes exactly one rule verifyColumn keeps: the column may already exist
// with a wider type.
//
// ADD COLUMN IF NOT EXISTS is a no-op for a column that is already there, and a
// replayed ADD arrives after the target moved forward:
//
//	ADD COLUMN c VARCHAR(32) → MODIFY c VARCHAR(64) → crash before the checkpoint
//
// Replay starts at the ADD, but the target already holds VARCHAR(64), which
// stores every value VARCHAR(32) does. Refusing it would stop a job whose
// target is ahead of the statement it is replaying, so the read-back accepts a
// target type that the same verified widening ALTER COLUMN TYPE accepts, read
// in the other direction (safeAlterType(want, live)). A column this sink just
// created always matches exactly and never reaches that branch.
//
// The relaxation is one-directional and stays proved: a target that is narrower
// than the source mapping, of another family, or neither is still refused, and
// so is a target that is stricter about NULL than the source. A target that is
// more permissive about NULL (nullable where the source is NOT NULL) accepts
// every row the source sends and is what a replayed MODIFY leaves behind, so it
// is accepted; a replayed MODIFY that must precede it tightens the column again.
func (s *Sink) verifyAddedColumn(ctx context.Context, target Target, column schema.Column) (tableState, error) {
	wantType, err := mapColumnType(column.Type)
	if err != nil {
		return tableState{}, fmt.Errorf("sink.postgresql: table %s column %s: %w", target, column.Name, err)
	}
	state, exists, err := s.meta.refresh(ctx, target)
	if err != nil {
		return tableState{}, err
	}
	if !exists {
		return tableState{}, fmt.Errorf("sink.postgresql: table %s does not exist after the schema change", target)
	}
	live, ok := state.columnFold(column.Name)
	if !ok {
		return tableState{}, fmt.Errorf("sink.postgresql: column %q is missing from %s after the schema change", column.Name, target)
	}
	if !samePGType(wantType, live.Type) {
		if err := safeAlterType(wantType, live.Type); err != nil {
			return tableState{}, fmt.Errorf("sink.postgresql: column %q of %s is %s after the schema change, the source %s maps to %s; a target column may be wider than the source mapping but never narrower or of another family, and this replayed ADD COLUMN cannot make it hold the source values",
				column.Name, target, live.Type, column.Type, wantType)
		}
		s.logger().Warn("postgresql: added column already exists with a wider type; the replayed change left it unchanged",
			"table", target.String(), "column", column.Name, "target", live.Type, "source", wantType)
	}
	if live.NotNull && column.Nullable {
		return tableState{}, fmt.Errorf("sink.postgresql: column %q of %s is NOT NULL after the schema change while the source column is nullable",
			column.Name, target)
	}
	return state, nil
}

// verifyColumnAbsent proves a dropped column is gone.
func (s *Sink) verifyColumnAbsent(ctx context.Context, target Target, name string) error {
	state, exists, err := s.meta.refresh(ctx, target)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("sink.postgresql: table %s does not exist after dropping column %q", target, name)
	}
	if _, ok := state.columnFold(name); ok {
		return fmt.Errorf("sink.postgresql: column %q is still in %s after the drop", name, target)
	}
	return nil
}

// blockingDependents keeps the dependencies a DROP COLUMN must not take with
// it: keys, foreign keys and unique indexes. A plain index and a check
// constraint go with the column harmlessly.
func blockingDependents(dependents []dependent) []string {
	var out []string
	for _, entry := range dependents {
		switch entry.Kind {
		case "p", "u", "f", "ui":
			out = append(out, entry.Name+" ("+dependentKindName(entry.Kind)+")")
		}
	}
	return out
}

func dependentKindName(kind string) string {
	switch kind {
	case "p":
		return "primary key"
	case "u":
		return "unique constraint"
	case "f":
		return "foreign key"
	case "ui":
		return "unique index"
	case "c":
		return "check constraint"
	case "i":
		return "index"
	default:
		return "constraint"
	}
}

// restatesColumnDefinition reports whether the statement repeats the column
// definition, as every MySQL CHANGE and MODIFY clause does. A plain
// RENAME COLUMN moves the name and nothing else.
func restatesColumnDefinition(ddl string) bool {
	return hasKeyword(ddl, "CHANGE") || hasKeyword(ddl, "MODIFY")
}

// hasKeyword reports whether text contains keyword as a bare word. A quoted
// identifier (`change`) is not the keyword.
func hasKeyword(text, keyword string) bool {
	for i := 0; i+len(keyword) <= len(text); i++ {
		if !strings.EqualFold(text[i:i+len(keyword)], keyword) {
			continue
		}
		before := i == 0 || !isIdentifierByte(text[i-1])
		after := i+len(keyword) == len(text) || !isIdentifierByte(text[i+len(keyword)])
		if before && after {
			return true
		}
	}
	return false
}

func isIdentifierByte(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		return true
	case c == '_' || c == '$' || c == '`':
		return true
	}
	return false
}

// pgType is one parsed PostgreSQL type: the canonical keyword and the arguments
// of its type modifier.
type pgType struct {
	name string
	args []string
}

// parseIntType parses the spellings format_type returns and the spellings this
// package generates.
func parseIntType(text string) pgType {
	trimmed := strings.ToLower(strings.TrimSpace(text))
	name, args := trimmed, ""
	if open := strings.IndexByte(trimmed, '('); open >= 0 {
		name = strings.TrimSpace(trimmed[:open])
		if closing := strings.LastIndexByte(trimmed, ')'); closing > open {
			args = strings.TrimSpace(trimmed[open+1 : closing])
		}
	}
	if keyword, ok := pgTypeKeywords[name]; ok {
		name = keyword
	}
	parsed := pgType{name: name}
	if args == "" {
		return parsed
	}
	for _, part := range strings.Split(args, ",") {
		parsed.args = append(parsed.args, strings.TrimSpace(part))
	}
	return parsed
}

// integerDigits is how many decimal digits each integer type holds.
var integerDigits = map[string]int{"smallint": 5, "integer": 10, "bigint": 19}

// unbounded stands for the capacity of a type without a modifier: text,
// varchar without a length, and numeric without a precision.
const unbounded = math.MaxInt32

// capacity returns the integer digits and the fractional digits a numeric type
// can hold. An unbounded numeric is treated as unlimited in both directions, so
// narrowing it to any bounded precision is refused.
func (t pgType) capacity() (digits, scale int, ok bool) {
	if t.name != "numeric" {
		return 0, 0, false
	}
	switch len(t.args) {
	case 0:
		return unbounded, unbounded, true
	case 1:
		precision, err := strconv.Atoi(t.args[0])
		if err != nil {
			return 0, 0, false
		}
		return precision, 0, true
	case 2:
		precision, err := strconv.Atoi(t.args[0])
		if err != nil {
			return 0, 0, false
		}
		scale, err := strconv.Atoi(t.args[1])
		if err != nil {
			return 0, 0, false
		}
		return precision - scale, scale, true
	}
	return 0, 0, false
}

// length returns the character limit of char(n) or varchar(n), or unbounded for
// the spelling without a modifier.
func (t pgType) length() (int, bool) {
	switch t.name {
	case "text":
		return unbounded, true
	case "char", "varchar":
		switch len(t.args) {
		case 0:
			return unbounded, true
		case 1:
			n, err := strconv.Atoi(t.args[0])
			if err != nil {
				return 0, false
			}
			return n, true
		}
	}
	return 0, false
}

// timePrecision returns the fractional-seconds precision of timestamp(p) or
// time(p). A timestamp without a modifier stores microseconds.
func (t pgType) timePrecision() (int, bool) {
	if t.name != "timestamp" && t.name != "time" {
		return 0, false
	}
	switch len(t.args) {
	case 0:
		return maxTimePrecision, true
	case 1:
		n, err := strconv.Atoi(t.args[0])
		if err != nil {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// safeAlterType decides whether a live column of type from can be widened to
// type to without losing a value or changing its meaning.
//
// Only conversions PostgreSQL applies with an implicit assignment cast are
// allowed, so no USING clause is needed and nothing is reinterpreted. A
// conversion that narrows, crosses between unrelated families, or has no
// verified equivalent is refused instead of being attempted.
func safeAlterType(from, to string) error {
	a, b := parseIntType(from), parseIntType(to)
	if samePGType(from, to) {
		return nil
	}
	if ok := wideningAllowed(a, b); ok {
		return nil
	}
	return fmt.Errorf("ALTER COLUMN TYPE has no verified lossless conversion from %s to %s; apply it by hand after checking the data", from, to)
}

func wideningAllowed(a, b pgType) bool {
	// Integer to integer.
	if digitsA, ok := integerDigits[a.name]; ok {
		if digitsB, ok := integerDigits[b.name]; ok {
			return digitsB > digitsA
		}
		// Integer to numeric, when the integer part can still hold every value.
		if digitsB, scaleB, ok := b.capacity(); ok {
			return scaleB >= 0 && digitsB >= digitsA
		}
		return false
	}
	// Numeric to numeric, with neither the integer part nor the scale shrinking.
	if a.name == "numeric" && b.name == "numeric" {
		digitsA, scaleA, okA := a.capacity()
		digitsB, scaleB, okB := b.capacity()
		return okA && okB && digitsB >= digitsA && scaleB >= scaleA
	}
	// Character types: a longer limit, or text, which has none.
	if lengthA, ok := a.length(); ok {
		if b.name == "text" {
			return true
		}
		// varchar to char pads and trims, which changes the stored value; char to
		// varchar does not.
		if a.name == "char" || a.name == "varchar" {
			if lengthB, ok := b.length(); ok && b.name != "text" {
				if a.name == "varchar" && b.name == "char" {
					return false
				}
				return lengthB >= lengthA
			}
		}
		return false
	}
	// Floating point, where double precision holds every real.
	if a.name == "real" && b.name == "double precision" {
		return true
	}
	// Temporal precision can only grow. A date becomes the same instant at
	// midnight, which is what MySQL's date to datetime conversion means.
	if a.name == "date" && b.name == "timestamp" {
		return true
	}
	if precisionA, ok := a.timePrecision(); ok {
		if a.name == b.name {
			if precisionB, ok := b.timePrecision(); ok {
				return precisionB >= precisionA
			}
		}
		return false
	}
	return false
}

func containsFold(list []string, want string) bool {
	for _, item := range list {
		if strings.EqualFold(strings.TrimSpace(item), want) {
			return true
		}
	}
	return false
}
