package postgresql

import (
	"strings"
	"testing"
)

func TestMapColumnType(t *testing.T) {
	tests := []struct {
		mysqlType string
		want      string
	}{
		{"tinyint(4)", "SMALLINT"},
		{"tinyint(1)", "SMALLINT"},
		{"tinyint(3) unsigned", "SMALLINT"},
		{"smallint(6)", "INTEGER"},
		{"smallint(5) unsigned", "INTEGER"},
		{"mediumint(8)", "INTEGER"},
		{"int(11)", "INTEGER"},
		{"int", "INTEGER"},
		{"int(10) unsigned", "BIGINT"},
		{"bigint(20)", "BIGINT"},
		{"bigint(20) unsigned", "NUMERIC(20,0)"},
		{"bigint(20) unsigned zerofill", "NUMERIC(20,0)"},
		{"decimal(10,2)", "NUMERIC(10,2)"},
		{"decimal(10,0)", "NUMERIC(10)"},
		{"numeric(38,18)", "NUMERIC(38,18)"},
		{"dec(5,2)", "NUMERIC(5,2)"},
		{"float", "REAL"},
		{"float(7,2)", "REAL"},
		{"double", "DOUBLE PRECISION"},
		{"double(10,2)", "DOUBLE PRECISION"},
		{"real", "DOUBLE PRECISION"},
		{"char(32)", "CHAR(32)"},
		{"varchar(64)", "VARCHAR(64)"},
		{"varchar(20000000)", "TEXT"},
		{"text", "TEXT"},
		{"tinytext", "TEXT"},
		{"mediumtext", "TEXT"},
		{"longtext", "TEXT"},
		{"binary(16)", "BYTEA"},
		{"varbinary(255)", "BYTEA"},
		{"blob", "BYTEA"},
		{"longblob", "BYTEA"},
		{"date", "DATE"},
		{"datetime", "TIMESTAMP"},
		{"datetime(6)", "TIMESTAMP(6)"},
		{"timestamp", "TIMESTAMP"},
		{"timestamp(3)", "TIMESTAMP(3)"},
		{"year(4)", "SMALLINT"},
		{"json", "JSON"},
		// The canal fallback path spells some types without arguments or in
		// upper case, and both must still map.
		{"VARCHAR", "TEXT"},
		{"BIGINT UNSIGNED", "NUMERIC(20,0)"},
	}
	for _, tc := range tests {
		got, err := mapColumnType(tc.mysqlType)
		if err != nil {
			t.Errorf("%s: %v", tc.mysqlType, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s: got %s want %s", tc.mysqlType, got, tc.want)
		}
	}
}

// TestMapColumnTypeRefusesUnverifiedTypes pins the fail-closed rule: a type whose
// arriving value is not proven is refused at table creation rather than mapped to
// something that would store the wrong thing.
func TestMapColumnTypeRefusesUnverifiedTypes(t *testing.T) {
	for _, mysqlType := range []string{
		"enum('a','b')",
		"set('a','b')",
		"bit(8)",
		"time",
		"time(6)",
		"geometry",
		"point",
		"unknown_type(3)",
	} {
		got, err := mapColumnType(mysqlType)
		if err == nil {
			t.Fatalf("%s accepted as %s", mysqlType, got)
		}
		message := err.Error()
		if !strings.Contains(message, "sink.postgresql") || !strings.Contains(message, mysqlType) {
			t.Fatalf("%s: unhelpful error %q", mysqlType, message)
		}
	}
}

func TestMapColumnTypeRejectsOutOfRangeArguments(t *testing.T) {
	tests := []struct {
		mysqlType string
		why       string
	}{
		{"decimal(0,0)", "precision"},
		{"decimal(1001,2)", "precision"},
		{"decimal(5,6)", "scale larger than"},
		{"decimal(5,1001)", "scale"},
		{"varchar(0)", "cannot parse"},
		{"datetime(7)", "fractional precision"},
		{"int(11", "cannot parse"},
	}
	for _, tc := range tests {
		_, err := mapColumnType(tc.mysqlType)
		if err == nil || !strings.Contains(err.Error(), tc.why) {
			t.Fatalf("%s: got %v, want a complaint about %s", tc.mysqlType, err, tc.why)
		}
	}
}

func TestSplitMySQLType(t *testing.T) {
	base, args, unsigned, err := splitMySQLType("BIGINT(20) UNSIGNED ZEROFILL")
	if err != nil {
		t.Fatal(err)
	}
	if base != "bigint" || args != "20" || !unsigned {
		t.Fatalf("base %q args %q unsigned %v", base, args, unsigned)
	}
	base, args, unsigned, err = splitMySQLType("enum('a,b','c')")
	if err != nil {
		t.Fatal(err)
	}
	if base != "enum" || args != "'a,b','c'" || unsigned {
		t.Fatalf("base %q args %q unsigned %v", base, args, unsigned)
	}
}
