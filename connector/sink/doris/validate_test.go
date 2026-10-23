package doris

import (
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
		{Name: "name", Type: "VARCHAR(64)", Nullable: true},
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

func TestValidateTargetRejectsNarrowing(t *testing.T) {
	target := validTarget()
	target[1].Type = "VARCHAR(8)"
	if err := validateTarget(validateSource(), target); err == nil {
		t.Fatal("narrowing must fail")
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
	existing := descColumn{Name: "name", Type: "VARCHAR(64)", Nullable: true}
	if err := validateExistingColumn(existing, schema.Column{Name: "name", Type: "varchar(32)", Nullable: true}); err != nil {
		t.Fatal(err)
	}
	if err := validateExistingColumn(existing, schema.Column{Name: "name", Type: "varchar(128)", Nullable: true}); err == nil {
		t.Fatal("narrower target must fail")
	}
	if err := validateExistingColumn(existing, schema.Column{Name: "name", Type: "varchar(32)", Nullable: false}); err == nil {
		t.Fatal("nullability mismatch must fail")
	}
}
