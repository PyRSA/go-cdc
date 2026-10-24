package doris

import (
	"errors"
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/schema"
)

func validateSource() schema.Table {
	return schema.Table{
		Database: "db",
		Table:    "orders",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "name", Type: "varchar(32)", Nullable: true},
		},
		PrimaryKeys: []string{"id"},
	}
}

func validTarget() []descColumn {
	return []descColumn{
		{Name: "id", Type: "BIGINT", Nullable: false, Key: true},
		{Name: "name", Type: "VARCHAR(96)", Nullable: true},
	}
}

func TestValidateTargetAcceptsWidening(t *testing.T) {
	if err := validateTarget(validateSource(), validTarget()); err != nil {
		t.Fatal(err)
	}
}

func TestValidateTargetAllowsWritableExtras(t *testing.T) {
	target := append(validTarget(),
		descColumn{Name: "note", Type: "STRING", Nullable: true},
		descColumn{Name: "created_at", Type: "DATETIME(3)", Nullable: false, HasDefault: true},
		descColumn{Name: "empty_default", Type: "VARCHAR(8)", Nullable: false, Default: "", HasDefault: true},
		descColumn{Name: "computed", Type: "INT", Nullable: false, Extra: "VIRTUAL GENERATED"},
	)
	if err := validateTarget(validateSource(), target); err != nil {
		t.Fatal(err)
	}
}

func TestDescDefaultEmptyStringIsHasDefault(t *testing.T) {
	def, has := descDefault("")
	if !has || def != "" {
		t.Fatalf("empty string default: def=%q has=%v", def, has)
	}
	def, has = descDefault([]byte(""))
	if !has || def != "" {
		t.Fatalf("empty []byte default: def=%q has=%v", def, has)
	}
	def, has = descDefault(nil)
	if has || def != "" {
		t.Fatalf("nil default: def=%q has=%v", def, has)
	}
	def, has = descDefault("NULL")
	if has || def != "" {
		t.Fatalf("NULL token: def=%q has=%v", def, has)
	}
	def, has = descDefault("legacy")
	if !has || def != "legacy" {
		t.Fatalf("literal default: def=%q has=%v", def, has)
	}
}

func TestValidateTargetRejectsMissingColumn(t *testing.T) {
	target := validTarget()[:1]
	if err := validateTarget(validateSource(), target); err == nil || !strings.Contains(err.Error(), "missing column name") {
		t.Fatalf("err=%v", err)
	}
}

func TestValidateTargetReportsMissingColumnsAsRecoverable(t *testing.T) {
	// CreateTable downgrades exactly this error to a warning; everything else
	// must stay fatal.
	source := validateSource()
	source.Columns = append(source.Columns, schema.Column{Name: "note", Type: "varchar(32)", Nullable: true})
	var missing *missingColumnsError
	if err := validateTarget(source, validTarget()); !errors.As(err, &missing) {
		t.Fatalf("err=%v", err)
	}
	if len(missing.Columns) != 1 || missing.Columns[0] != "note" {
		t.Fatalf("columns %#v", missing.Columns)
	}
}

func TestValidateTargetRejectsNarrowing(t *testing.T) {
	target := validTarget()
	target[1].Type = "VARCHAR(95)"
	err := validateTarget(validateSource(), target)
	if err == nil {
		t.Fatal("narrowing must fail")
	}
	// The message must name the byte capacity the source needs so the operator
	// can widen the target without guessing (varchar(32) needs 96 bytes).
	if !strings.Contains(err.Error(), "VARCHAR(96)") {
		t.Fatalf("err=%v", err)
	}
}

func TestValidateTargetAcceptsNotNullSourceIntoNullableTarget(t *testing.T) {
	// A nullable target column may receive a NOT NULL source column. The
	// concurrent ADD COLUMN path (validateExistingColumn) applies the same
	// one-way rule, so both directions are pinned on both paths.
	source := validateSource()
	source.Columns[1].Nullable = false
	if err := validateTarget(source, validTarget()); err != nil {
		t.Fatalf("a nullable target must accept a NOT NULL source column: %v", err)
	}
}

func TestValidateTargetRejectsNullableIntoNotNull(t *testing.T) {
	target := validTarget()
	target[1].Nullable = false
	if err := validateTarget(validateSource(), target); err == nil || !strings.Contains(err.Error(), "NOT NULL") {
		t.Fatalf("err=%v", err)
	}
}

func TestValidateTargetRejectsMissingKey(t *testing.T) {
	target := validTarget()
	target[0].Key = false
	if err := validateTarget(validateSource(), target); err == nil || !strings.Contains(err.Error(), "UNIQUE KEY") {
		t.Fatalf("err=%v", err)
	}
}

func TestValidateTargetRejectsExtraKey(t *testing.T) {
	target := append(validTarget(), descColumn{Name: "tenant", Type: "VARCHAR(8)", Nullable: false, Key: true})
	if err := validateTarget(validateSource(), target); err == nil || !strings.Contains(err.Error(), "not a source primary key") {
		t.Fatalf("err=%v", err)
	}
}

func TestValidateTargetRejectsRequiredExtra(t *testing.T) {
	target := append(validTarget(), descColumn{Name: "required", Type: "INT", Nullable: false})
	if err := validateTarget(validateSource(), target); err == nil || !strings.Contains(err.Error(), "extra column") {
		t.Fatalf("err=%v", err)
	}
}

func TestValidateTargetRejectsTableWithoutKey(t *testing.T) {
	source := validateSource()
	source.PrimaryKeys = nil
	if err := validateTarget(source, validTarget()); err == nil {
		t.Fatal("source without primary key must fail")
	}
}

func TestValidateTargetRejectsUnsupportedSourceType(t *testing.T) {
	source := validateSource()
	source.Columns[1].Type = "blob"
	if err := validateTarget(source, validTarget()); err == nil {
		t.Fatal("blob source must fail")
	}
}

func TestValidateExistingColumn(t *testing.T) {
	existing := descColumn{Name: "name", Type: "VARCHAR(96)", Nullable: true}
	if err := validateExistingColumn(existing, schema.Column{Name: "name", Type: "varchar(32)", Nullable: true}); err != nil {
		t.Fatal(err)
	}
	if err := validateExistingColumn(existing, schema.Column{Name: "name", Type: "varchar(128)", Nullable: true}); err == nil {
		t.Fatal("narrower target must fail")
	}
	// A nullable target accepts a NOT NULL source column: validateTarget applies
	// the same one-way rule, so both paths must agree.
	if err := validateExistingColumn(existing, schema.Column{Name: "name", Type: "varchar(32)", Nullable: false}); err != nil {
		t.Fatalf("a nullable target must accept a NOT NULL source column: %v", err)
	}
	strict := descColumn{Name: "name", Type: "VARCHAR(96)", Nullable: false}
	if err := validateExistingColumn(strict, schema.Column{Name: "name", Type: "varchar(32)", Nullable: true}); err == nil {
		t.Fatal("a NOT NULL target must reject a nullable source column")
	}
}

func TestParseTableModelUniqueKey(t *testing.T) {
	ddl := "CREATE TABLE `db`.`orders` (\n  `tenant` VARCHAR(16) NOT NULL,\n  `id` BIGINT NOT NULL\n" +
		") ENGINE=OLAP\nUNIQUE KEY(`tenant`, `id`)\nDISTRIBUTED BY HASH(`tenant`) BUCKETS auto"
	model, err := parseTableModel(ddl)
	if err != nil {
		t.Fatal(err)
	}
	if !model.UniqueKey {
		t.Fatal("expected a UNIQUE KEY model")
	}
	want := []string{"tenant", "id"}
	if len(model.Keys) != len(want) || model.Keys[0] != want[0] || model.Keys[1] != want[1] {
		t.Fatalf("keys %#v", model.Keys)
	}
}

func TestParseTableModelRejectsOtherModels(t *testing.T) {
	duplicate := "CREATE TABLE `db`.`orders` (`id` BIGINT NOT NULL) ENGINE=OLAP\nDUPLICATE KEY(`id`) BUCKETS auto"
	if _, err := parseTableModel(duplicate); err == nil || !strings.Contains(err.Error(), "DUPLICATE KEY") {
		t.Fatalf("duplicate err=%v", err)
	}
	aggregate := "CREATE TABLE `db`.`orders` (`id` BIGINT NOT NULL) ENGINE=OLAP\nAGGREGATE KEY(`id`) BUCKETS auto"
	if _, err := parseTableModel(aggregate); err == nil || !strings.Contains(err.Error(), "AGGREGATE KEY") {
		t.Fatalf("aggregate err=%v", err)
	}
	unknown := "CREATE TABLE `db`.`orders` (`id` BIGINT NOT NULL) ENGINE=OLAP"
	if _, err := parseTableModel(unknown); err == nil {
		t.Fatal("an unknown model must not be treated as compatible")
	}
}

func TestParseTableModelIgnoresQuotedText(t *testing.T) {
	// A column comment mentioning UNIQUE KEY must not be read as the model.
	ddl := "CREATE TABLE `db`.`orders` (`id` BIGINT NOT NULL COMMENT 'UNIQUE KEY(`id`)') " +
		"ENGINE=OLAP\nDUPLICATE KEY(`id`) BUCKETS auto"
	if _, err := parseTableModel(ddl); err == nil || !strings.Contains(err.Error(), "DUPLICATE KEY") {
		t.Fatalf("err=%v", err)
	}
}

func TestValidateTableModelEnforcesKeyOrder(t *testing.T) {
	source := schema.Table{
		Database: "db", Table: "orders", PrimaryKeys: []string{"tenant", "id"},
		Columns: []schema.Column{{Name: "tenant", Type: "varchar(16)"}, {Name: "id", Type: "bigint"}},
	}
	if err := validateTableModel(source, tableModel{UniqueKey: true, Keys: []string{"tenant", "id"}}); err != nil {
		t.Fatal(err)
	}
	err := validateTableModel(source, tableModel{UniqueKey: true, Keys: []string{"id", "tenant"}})
	if err == nil || !strings.Contains(err.Error(), "order") {
		t.Fatalf("err=%v", err)
	}
	err = validateTableModel(source, tableModel{UniqueKey: true, Keys: []string{"tenant"}})
	if err == nil || !strings.Contains(err.Error(), "does not match source primary key") {
		t.Fatalf("err=%v", err)
	}
	err = validateTableModel(source, tableModel{})
	if err == nil || !strings.Contains(err.Error(), "not a UNIQUE KEY table") {
		t.Fatalf("err=%v", err)
	}
}
