package mysql

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/schema"
)

// TestColumnDefaultDiscoveryMapping mirrors db.go column default wiring
// (HasDefault from NullString.Valid, DefaultExpr from DEFAULT_GENERATED).
func TestColumnDefaultDiscoveryMapping(t *testing.T) {
	cases := []struct {
		name       string
		colDefault sql.NullString
		extra      string
		wantDef    string
		wantHas    bool
		wantExpr   bool
	}{
		{"no default", sql.NullString{}, "", "", false, false},
		{"empty string", sql.NullString{Valid: true, String: ""}, "", "", true, false},
		{"literal", sql.NullString{Valid: true, String: "x"}, "", "x", true, false},
		{"generated expr", sql.NullString{Valid: true, String: "(1 + 1)"}, "DEFAULT_GENERATED", "(1 + 1)", true, true},
		{"generated case", sql.NullString{Valid: true, String: "uuid()"}, "default_generated", "uuid()", true, true},
	}
	for _, tc := range cases {
		def := nullString(tc.colDefault)
		has := tc.colDefault.Valid
		expr := strings.Contains(strings.ToUpper(tc.extra), "DEFAULT_GENERATED")
		if def != tc.wantDef || has != tc.wantHas || expr != tc.wantExpr {
			t.Fatalf("%s: got def=%q has=%v expr=%v want def=%q has=%v expr=%v",
				tc.name, def, has, expr, tc.wantDef, tc.wantHas, tc.wantExpr)
		}
	}
}

func TestIndexColumnFromStats(t *testing.T) {
	name, prefix, ok := indexColumnFromStats(sql.NullString{String: "name", Valid: true}, sql.NullInt64{})
	if !ok || name != "name" || prefix != 0 {
		t.Fatalf("plain column: %q %d %v", name, prefix, ok)
	}
	name, prefix, ok = indexColumnFromStats(sql.NullString{String: "name", Valid: true}, sql.NullInt64{Int64: 16, Valid: true})
	if !ok || name != "name" || prefix != 16 {
		t.Fatalf("prefix: %q %d %v", name, prefix, ok)
	}
	_, _, ok = indexColumnFromStats(sql.NullString{}, sql.NullInt64{})
	if ok {
		t.Fatal("NULL column_name (functional index) must not be ok")
	}
	_, _, ok = indexColumnFromStats(sql.NullString{Valid: true, String: ""}, sql.NullInt64{})
	if ok {
		t.Fatal("empty column_name must not be ok")
	}
}

// TestIndexStatsRowsBuildSecondaryUniqueCompositePrefix mirrors fillIndexes
// accumulation without a live MySQL: one functional part must drop the index
// and emit an explicit discovery warning.
func TestIndexStatsRowsBuildSecondaryUniqueCompositePrefix(t *testing.T) {
	type row struct {
		indexName string
		unique    bool
		column    sql.NullString
		subPart   sql.NullInt64
		indexType string
	}
	rows := []row{
		{indexName: "idx_a", column: sql.NullString{String: "a", Valid: true}, indexType: "BTREE"},
		{indexName: "uk_b", unique: true, column: sql.NullString{String: "b", Valid: true}, indexType: "BTREE"},
		{indexName: "idx_ab", column: sql.NullString{String: "a", Valid: true}, indexType: "BTREE"},
		{indexName: "idx_ab", column: sql.NullString{String: "b", Valid: true}, subPart: sql.NullInt64{Int64: 8, Valid: true}, indexType: "BTREE"},
		{indexName: "idx_prefix", column: sql.NullString{String: "c", Valid: true}, subPart: sql.NullInt64{Int64: 16, Valid: true}, indexType: "BTREE"},
		{indexName: "idx_func", column: sql.NullString{}, indexType: "BTREE"}, // NULL column_name
	}
	pending := map[string]*indexAcc{}
	for _, r := range rows {
		item := pending[r.indexName]
		if item == nil {
			item = &indexAcc{idx: schema.Index{Name: r.indexName, Unique: r.unique, Type: r.indexType}}
			pending[r.indexName] = item
		}
		if item.skip {
			continue
		}
		name, prefix, ok := indexColumnFromStats(r.column, r.subPart)
		if !ok {
			item.skip = true
			item.reason = "functional index"
			item.idx.Columns = nil
			item.idx.Prefixes = nil
			continue
		}
		item.idx.Columns = append(item.idx.Columns, name)
		item.idx.Prefixes = append(item.idx.Prefixes, prefix)
	}
	tab := &schema.Table{Database: "db", Table: "t"}
	var warns []string
	attachDiscoveredIndexes(tab, pending, func(msg string, args ...any) {
		warns = append(warns, msg)
		_ = args
	})
	got := tab.Indexes
	if len(got) != 4 {
		t.Fatalf("want 4 indexes (func skipped), got %#v", got)
	}
	byName := map[string]schema.Index{}
	for _, idx := range got {
		byName[idx.Name] = idx
	}
	if !byName["uk_b"].Unique || len(byName["idx_ab"].Columns) != 2 || byName["idx_ab"].Prefixes[1] != 8 {
		t.Fatalf("%#v", byName)
	}
	if byName["idx_prefix"].Prefixes[0] != 16 {
		t.Fatalf("prefix %#v", byName["idx_prefix"])
	}
	if _, ok := byName["idx_func"]; ok {
		t.Fatal("functional index must be skipped")
	}
	if len(warns) != 1 || !strings.Contains(warns[0], "skipping unsupported index") {
		t.Fatalf("expected explicit skip warning, got %#v", warns)
	}
}
