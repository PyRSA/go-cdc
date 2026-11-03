package mysql

import (
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
)

func TestUnsupportedColumnType(t *testing.T) {
	tests := []struct {
		mysqlType string
		want      bool
	}{
		{mysqlType: "vector(3)", want: true},
		{mysqlType: "VECTOR(1536)", want: true},
		{mysqlType: "vector", want: true},
		// The classifier keys on the bare type name: a column whose name
		// merely contains "vector" must not be caught.
		{mysqlType: "vector_data", want: false},
		{mysqlType: "varbinary(16)", want: false},
		{mysqlType: "binary(16)", want: false},
		{mysqlType: "longblob", want: false},
		{mysqlType: "text", want: false},
		{mysqlType: "bigint unsigned", want: false},
		{mysqlType: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.mysqlType, func(t *testing.T) {
			if got := unsupportedColumnType(tt.mysqlType); got != tt.want {
				t.Fatalf("unsupportedColumnType(%q) = %v, want %v", tt.mysqlType, got, tt.want)
			}
		})
	}
}

// TestVectorIsOutsideTheBinaryClassifier pins the reason the table guard exists:
// isBinaryType does not cover VECTOR, so the snapshot path would hand the column
// back as a string while the binlog path keeps its bytes.
func TestVectorIsOutsideTheBinaryClassifier(t *testing.T) {
	if isBinaryType("vector(3)") {
		t.Fatal("vector must not be classified as binary; the guard exists because it is not")
	}
	if !unsupportedColumnType("vector(3)") {
		t.Fatal("vector must be rejected by the catalogue guard")
	}
}

func TestRejectUnsupportedColumns(t *testing.T) {
	selector, err := definition.CompileSelector(`shop\..*`, `shop\.archive_.*`)
	if err != nil {
		t.Fatalf("CompileSelector: %v", err)
	}
	db := &sqlDB{cfg: definition.Source{Tables: selector}}

	ordinary := schema.Table{
		Database: "shop", Table: "orders",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint unsigned"},
			{Name: "payload", Type: "longblob"},
		},
	}
	vectors := schema.Table{
		Database: "shop", Table: "items",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint"},
			{Name: "embedding", Type: "vector(1536)"},
		},
	}
	archived := schema.Table{
		Database: "shop", Table: "archive_items",
		Columns: []schema.Column{{Name: "embedding", Type: "vector(3)"}},
	}

	t.Run("ordinary table passes", func(t *testing.T) {
		if err := db.rejectUnsupportedColumns([]schema.Table{ordinary}, false); err != nil {
			t.Fatalf("rejectUnsupportedColumns: %v", err)
		}
		if err := db.rejectUnsupportedColumns([]schema.Table{ordinary}, true); err != nil {
			t.Fatalf("explicit describe of an ordinary table: %v", err)
		}
	})

	t.Run("vector column names database table and column", func(t *testing.T) {
		err := db.rejectUnsupportedColumns([]schema.Table{ordinary, vectors}, false)
		if err == nil {
			t.Fatal("a captured table with a VECTOR column must be rejected")
		}
		for _, want := range []string{"shop.items.embedding", "vector(1536)"} {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("error %q does not mention %q", err, want)
			}
		}
	})

	t.Run("vector table outside the selector is left alone", func(t *testing.T) {
		// ListTables sees every non-system table; a VECTOR table the pipeline
		// does not capture must not stop the job.
		if err := db.rejectUnsupportedColumns([]schema.Table{archived}, false); err != nil {
			t.Fatalf("excluded table must not fail ListTables: %v", err)
		}
	})

	t.Run("explicit describe rejects excluded table", func(t *testing.T) {
		// DescribeTable only runs for a table that is being captured (binlog
		// CREATE refresh, resume revalidation), so the selector no longer
		// decides.
		if err := db.rejectUnsupportedColumns([]schema.Table{archived}, true); err == nil {
			t.Fatal("explicit describe of a VECTOR table must be rejected")
		}
	})

	t.Run("no selector configured", func(t *testing.T) {
		// A nil selector cannot prove the table is out of scope, and the guard
		// must not become a no-op.
		unset := &sqlDB{}
		if err := unset.rejectUnsupportedColumns([]schema.Table{archived}, true); err == nil {
			t.Fatal("explicit describe must be rejected even without a selector")
		}
	})
}
