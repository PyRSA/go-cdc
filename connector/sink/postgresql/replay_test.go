package postgresql

import (
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/schema"
)

// liveTable builds the state a catalogue read returns for one table. The
// columns are the key columns the tests use, so they are also the primary key.
func liveTable(schemaName, name string, columns ...columnState) tableState {
	var keys []string
	for _, column := range columns {
		keys = append(keys, column.Name)
	}
	return tableState{Schema: schemaName, Name: name, Columns: columns, PrimaryKeys: keys}
}

func TestValidateExistingTargetAcceptsMissingColumn(t *testing.T) {
	// A replayed CREATE TABLE carries the source definition as it stands now; a
	// later ADD COLUMN that has not been replayed yet leaves the target behind.
	table := schema.Table{
		Database: "shop", Table: "orders",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint"},
			{Name: "note", Type: "varchar(32)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	live := liveTable("shop", "orders",
		columnState{Name: "id", Type: "bigint", NotNull: true})
	if err := validateExistingTarget(table, live); err != nil {
		t.Fatalf("missing column must be accepted on an existing target: %v", err)
	}
}

func TestValidateExistingTargetStillRefusesATypeMismatch(t *testing.T) {
	table := schema.Table{
		Database: "shop", Table: "orders",
		Columns:     []schema.Column{{Name: "id", Type: "bigint"}},
		PrimaryKeys: []string{"id"},
	}
	live := liveTable("shop", "orders",
		columnState{Name: "id", Type: "text", NotNull: true})
	err := validateExistingTarget(table, live)
	if err == nil {
		t.Fatal("a target column of another type must still fail")
	}
	if strings.Contains(err.Error(), "is missing") {
		t.Fatalf("the failure must name the type, got %v", err)
	}
}

func TestValidateExistingTargetStillRefusesMissingKey(t *testing.T) {
	table := schema.Table{
		Database: "shop", Table: "orders",
		Columns:     []schema.Column{{Name: "id", Type: "bigint"}},
		PrimaryKeys: []string{"id"},
	}
	live := tableState{
		Schema: "shop", Name: "orders",
		Columns: []columnState{{Name: "id", Type: "bigint", NotNull: true}},
	}
	if err := validateExistingTarget(table, live); err == nil {
		t.Fatal("a missing conflict target must still fail")
	}
}

func TestValidateTargetTableStillRefusesMissingColumn(t *testing.T) {
	// The strict path is unchanged: a table this process just created has to
	// match the definition exactly, so a concurrent creator is still reported.
	table := schema.Table{
		Database: "shop", Table: "orders",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint"},
			{Name: "note", Type: "varchar(32)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	live := liveTable("shop", "orders",
		columnState{Name: "id", Type: "bigint", NotNull: true})
	err := validateTargetTable(table, live)
	if err == nil || !strings.Contains(err.Error(), `column "note" is missing`) {
		t.Fatalf("err=%v", err)
	}
}

func TestTargetTableProblemsSeparatesMissingFromFatal(t *testing.T) {
	table := schema.Table{
		Database: "shop", Table: "orders",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint"},
			{Name: "note", Type: "varchar(32)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
	live := liveTable("shop", "orders",
		columnState{Name: "id", Type: "text", NotNull: true})
	problems, err := targetTableProblems(table, live)
	if err != nil {
		t.Fatal(err)
	}
	if len(problems.missing) != 1 {
		t.Fatalf("missing %v", problems.missing)
	}
	if len(problems.fatal()) != 1 {
		t.Fatalf("fatal %v", problems.fatal())
	}
}
