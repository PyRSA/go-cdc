package doris

import (
	"strings"
	"testing"
)

func TestMapColumnType(t *testing.T) {
	cases := []struct{ raw, want string }{
		{"tinyint", "TINYINT"},
		{"tinyint(1)", "TINYINT"},
		{"tinyint unsigned", "SMALLINT"},
		{"smallint(6)", "SMALLINT"},
		{"smallint unsigned", "INT"},
		{"mediumint(9)", "INT"},
		{"mediumint unsigned", "INT"},
		{"int(11)", "INT"},
		{"int unsigned", "BIGINT"},
		{"bigint(20)", "BIGINT"},
		{"bigint(20) unsigned", "LARGEINT"},
		{"float", "FLOAT"},
		{"double", "DOUBLE"},
		{"real", "DOUBLE"},
		{"decimal(10,2)", "DECIMAL(10,2)"},
		{"decimal(38,0)", "DECIMAL(38,0)"},
		{"numeric(5,1)", "DECIMAL(5,1)"},
		{"char(3)", "CHAR(9)"},
		{"varchar(255)", "VARCHAR(765)"},
		{"tinytext", "STRING"},
		{"text", "STRING"},
		{"mediumtext", "STRING"},
		{"longtext", "STRING"},
		{"date", "DATE"},
		{"datetime", "DATETIME(0)"},
		{"datetime(6)", "DATETIME(6)"},
		{"timestamp", "DATETIME(0)"},
		{"timestamp(3)", "DATETIME(3)"},
		{"year", "SMALLINT"},
		{"json", "JSONB"},
	}
	for _, tc := range cases {
		mapped, err := mapColumnType(tc.raw)
		if err != nil {
			t.Fatalf("%s: %v", tc.raw, err)
		}
		if mapped.Doris != tc.want {
			t.Fatalf("%s: got %s want %s", tc.raw, mapped.Doris, tc.want)
		}
	}
}

// TestStringLengthsAreDorisByteCapacities pins the character-to-byte widening
// and the degrade path that keeps generated DDL inside the Doris type limits.
func TestStringLengthsAreDorisByteCapacities(t *testing.T) {
	cases := []struct {
		raw        string
		wantDoris  string
		wantLength int
	}{
		{"varchar(1)", "VARCHAR(3)", 3},
		{"varchar(85)", "VARCHAR(255)", 255},
		{"varchar(21844)", "VARCHAR(65532)", 65532},
		{"varchar(21845)", "STRING", 0},
		{"char(85)", "CHAR(255)", 255},
		{"char(86)", "VARCHAR(258)", 258},
		{"char(300)", "VARCHAR(900)", 900},
	}
	for _, tc := range cases {
		mapped, err := mapColumnType(tc.raw)
		if err != nil {
			t.Fatalf("%s: %v", tc.raw, err)
		}
		if mapped.Doris != tc.wantDoris || mapped.Length != tc.wantLength {
			t.Fatalf("%s: got %s length=%d, want %s length=%d",
				tc.raw, mapped.Doris, mapped.Length, tc.wantDoris, tc.wantLength)
		}
	}
}

func TestMapColumnTypeRejectsUnsafeTypes(t *testing.T) {
	cases := []string{
		"binary(16)", "varbinary(10)", "tinyblob", "blob", "mediumblob", "longblob",
		"enum('a','b')", "set('a','b')", "bit(8)", "time", "geometry", "point",
		"decimal(39,0)", "decimal(5,6)", "decimal", "varchar", "char(0)",
		"datetime(7)", "", "unknown_type",
	}
	for _, raw := range cases {
		if mapped, err := mapColumnType(raw); err == nil {
			t.Fatalf("%q should be rejected, got %s", raw, mapped.Doris)
		}
	}
}

func TestCompatible(t *testing.T) {
	cases := []struct {
		source string
		target string
		want   bool
	}{
		{"int", "INT", true},
		{"int", "BIGINT", true},
		{"bigint", "INT", false},
		{"int unsigned", "BIGINT", true},
		{"bigint unsigned", "LARGEINT", true},
		{"bigint unsigned", "BIGINT", false},
		{"varchar(32)", "VARCHAR(96)", true},
		{"varchar(32)", "VARCHAR(64)", false},
		{"varchar(64)", "VARCHAR(32)", false},
		{"varchar(32)", "STRING", true},
		{"text", "STRING", true},
		{"text", "TEXT", true},
		{"varchar(32)", "TEXT", true},
		{"text", "VARCHAR(1024)", false},
		{"decimal(10,2)", "DECIMAL(12,2)", true},
		{"decimal(10,2)", "DECIMAL(10,1)", false},
		{"decimal(10,2)", "DECIMAL(10,3)", false},
		{"decimal(10,2)", "DECIMAL(11,3)", true},
		{"char(8)", "VARCHAR(24)", true},
		{"char(8)", "VARCHAR(4)", false},
		{"varchar(30000)", "VARCHAR(65533)", false},
		{"varchar(30000)", "STRING", true},
		{"datetime(6)", "DATETIME(6)", true},
		{"datetime(6)", "DATETIME(3)", false},
		{"datetime", "DATETIME", true},
		{"date", "DATE", true},
		{"date", "DATEV2", true},
		{"date", "DATETIME", false},
		{"json", "JSONB", true},
		{"json", "STRING", false},
		{"float", "DOUBLE", true},
		{"double", "FLOAT", false},
		{"int", "FLOAT", false},
	}
	for _, tc := range cases {
		mapped, err := mapColumnType(tc.source)
		if err != nil {
			t.Fatalf("%s: %v", tc.source, err)
		}
		if got := compatible(mapped, tc.target); got != tc.want {
			t.Fatalf("%s -> %s: got %v want %v", tc.source, tc.target, got, tc.want)
		}
	}
}

func TestMapDorisType(t *testing.T) {
	cases := []struct{ raw, want string }{
		{"BIGINT", "BIGINT"},
		{"VARCHAR(64)", "VARCHAR(64)"},
		{"DECIMAL(12,2)", "DECIMAL(12,2)"},
		{"DATETIME(6)", "DATETIME(6)"},
		{"DATEV2", "DATEV2"},
		{"JSON", "JSONB"},
		{"STRING", "STRING"},
		{"TEXT", "STRING"},
		{"text", "STRING"},
		{"JSONB", "JSONB"},
	}
	for _, tc := range cases {
		mapped, err := mapDorisType(tc.raw)
		if err != nil {
			t.Fatalf("%s: %v", tc.raw, err)
		}
		if mapped.Doris != tc.want {
			t.Fatalf("%s: got %s want %s", tc.raw, mapped.Doris, tc.want)
		}
	}
	for _, raw := range []string{"", "ARRAY<INT>", "HLL", "BITMAP", "VARCHAR"} {
		if _, err := mapDorisType(raw); err == nil {
			t.Fatalf("%q should be rejected", raw)
		}
	}
}

func TestMapColumnTypeErrorMessageNamesType(t *testing.T) {
	_, err := mapColumnType("blob")
	if err == nil || !strings.Contains(err.Error(), "blob") {
		t.Fatalf("err=%v", err)
	}
}
