package elasticsearch

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/schema"
)

func TestParseColumnType(t *testing.T) {
	cases := []struct {
		raw      string
		base     string
		unsigned bool
		length   int
		scale    int
		fsp      int
		values   []string
	}{
		{raw: "bigint(20) unsigned", base: "bigint", unsigned: true, length: 20, scale: -1},
		{raw: "int(10) unsigned zerofill", base: "int", unsigned: true, length: 10, scale: -1},
		{raw: "decimal(10,2)", base: "decimal", length: 10, scale: 2},
		{raw: "varchar(600)", base: "varchar", length: 600, scale: -1},
		{raw: "datetime(6)", base: "datetime", length: -1, scale: -1, fsp: 6},
		{raw: "text", base: "text", length: -1, scale: -1},
		{raw: "enum('a','it''s','x\\'y')", base: "enum", length: -1, scale: -1, values: []string{"a", "it's", "x'y"}},
		{raw: "set('a','b')", base: "set", length: -1, scale: -1, values: []string{"a", "b"}},
	}
	for _, tc := range cases {
		got, err := parseColumnType(tc.raw)
		if err != nil {
			t.Fatalf("parseColumnType(%q): %v", tc.raw, err)
		}
		if got.Base != tc.base || got.Unsigned != tc.unsigned || got.Length != tc.length ||
			got.Scale != tc.scale || got.Fsp != tc.fsp {
			t.Errorf("parseColumnType(%q) = %+v", tc.raw, got)
		}
		if len(got.Values) != len(tc.values) {
			t.Errorf("parseColumnType(%q) values = %v, want %v", tc.raw, got.Values, tc.values)
			continue
		}
		for i := range tc.values {
			if got.Values[i] != tc.values[i] {
				t.Errorf("parseColumnType(%q) values = %v, want %v", tc.raw, got.Values, tc.values)
				break
			}
		}
	}
	for _, raw := range []string{"", "int bogus", "datetime(7)", "enum('a'"} {
		if _, err := parseColumnType(raw); err == nil {
			t.Errorf("parseColumnType(%q): expected an error", raw)
		}
	}
}

func TestDerivePropertiesTypeTable(t *testing.T) {
	table := schema.Table{
		Database: "shop",
		Table:    "types",
		Columns: []schema.Column{
			{Name: "ID", Type: "bigint(20) unsigned"},
			{Name: "Flag", Type: "tinyint(1)"},
			{Name: "Score", Type: "tinyint(3) unsigned"},
			{Name: "Small", Type: "smallint(5) unsigned"},
			{Name: "Medium", Type: "mediumint(8)"},
			{Name: "Count", Type: "int(11)"},
			{Name: "CountU", Type: "int(10) unsigned"},
			{Name: "Ratio", Type: "float"},
			{Name: "Total", Type: "double"},
			{Name: "Amount", Type: "decimal(10,2)"},
			{Name: "Day", Type: "date"},
			{Name: "Seen", Type: "datetime(6)"},
			{Name: "Updated", Type: "timestamp"},
			{Name: "Born", Type: "year"},
			{Name: "Alarm", Type: "time(3)"},
			{Name: "Name", Type: "varchar(100)"},
			{Name: "Body", Type: "varchar(600)"},
			{Name: "Note", Type: "text"},
			{Name: "Meta", Type: "json"},
			{Name: "Blob", Type: "blob"},
			{Name: "Status", Type: "enum('new','done')"},
			{Name: "Tags", Type: "set('a','b')"},
		},
	}
	props, err := deriveProperties(table)
	if err != nil {
		t.Fatalf("deriveProperties: %v", err)
	}
	want := map[string]esField{
		"ID":      {Type: "unsigned_long"},
		"Flag":    {Type: "byte"},
		"Score":   {Type: "short"},
		"Small":   {Type: "integer"},
		"Medium":  {Type: "integer"},
		"Count":   {Type: "integer"},
		"CountU":  {Type: "long"},
		"Ratio":   {Type: "float"},
		"Total":   {Type: "double"},
		"Amount":  {Type: stringKeyword},
		"Day":     {Type: "date", Format: dateFormat},
		"Seen":    {Type: "date", Format: datetimeFormat},
		"Updated": {Type: "date", Format: datetimeFormat},
		"Born":    {Type: "date", Format: yearFormat},
		"Alarm":   {Type: stringKeyword},
		"Name":    {Type: stringKeyword},
		"Body":    {Type: stringText},
		"Note":    {Type: stringText},
		"Meta":    {Type: "object", Dynamic: boolPtr(true)},
		"Blob":    {Type: "binary"},
		"Status":  {Type: stringKeyword},
		"Tags":    {Type: stringKeyword},
	}
	for name, expected := range want {
		raw, ok := props[name]
		if !ok {
			t.Errorf("column %s missing from derived properties", name)
			continue
		}
		var got esField
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Errorf("column %s: %v", name, err)
			continue
		}
		if got.Type != expected.Type || got.Format != expected.Format {
			t.Errorf("column %s = %+v, want %+v", name, got, expected)
		}
		if (got.Dynamic == nil) != (expected.Dynamic == nil) ||
			(got.Dynamic != nil && *got.Dynamic != *expected.Dynamic) {
			t.Errorf("column %s dynamic = %v, want %v", name, got.Dynamic, expected.Dynamic)
		}
	}
}

func TestDerivePropertiesRejectsUnsupportedTypes(t *testing.T) {
	cases := []schema.Column{
		{Name: "Bits", Type: "bit(8)"},
		{Name: "Unknown", Type: "geometry"},
		{Name: "Broken", Type: "int nonsense"},
	}
	for _, column := range cases {
		table := schema.Table{Database: "shop", Table: "bad", Columns: []schema.Column{column}}
		_, err := deriveProperties(table)
		if err == nil {
			t.Fatalf("column %q type %q: expected an error", column.Name, column.Type)
		}
		if !strings.Contains(err.Error(), column.Name) || !strings.Contains(err.Error(), "shop.bad") {
			t.Errorf("error %q must name the table and column", err)
		}
	}
}

func TestCheckCompatibilityAcceptsDerivedMapping(t *testing.T) {
	table := typesTable()
	props, err := deriveProperties(table)
	if err != nil {
		t.Fatalf("deriveProperties: %v", err)
	}
	if err := checkCompatibility("shop.types", table, props, ""); err != nil {
		t.Fatalf("derived mapping must be compatible: %v", err)
	}
}

func TestCheckCompatibilityRejectsConflicts(t *testing.T) {
	cases := []struct {
		name    string
		column  schema.Column
		props   string
		dynamic string
		want    string
	}{
		{
			name:   "unsigned bigint in signed long",
			column: schema.Column{Name: "c", Type: "bigint(20) unsigned"},
			props:  `{"c":{"type":"long"}}`,
			want:   "range",
		},
		{
			name:   "signed int in unsigned_long",
			column: schema.Column{Name: "c", Type: "int(11)"},
			props:  `{"c":{"type":"unsigned_long"}}`,
			want:   "unsigned_long",
		},
		{
			name:   "decimal in a numeric field",
			column: schema.Column{Name: "c", Type: "decimal(10,2)"},
			props:  `{"c":{"type":"double"}}`,
			want:   "precision",
		},
		{
			name:   "date without format",
			column: schema.Column{Name: "c", Type: "datetime(6)"},
			props:  `{"c":{"type":"date"}}`,
			want:   "format",
		},
		{
			name:   "date with another format",
			column: schema.Column{Name: "c", Type: "date"},
			props:  `{"c":{"type":"date","format":"epoch_millis"}}`,
			want:   "epoch_millis",
		},
		{
			name:    "json object under strict root",
			column:  schema.Column{Name: "c", Type: "json"},
			props:   `{"c":{"type":"object"}}`,
			dynamic: "strict",
			want:    "dynamic",
		},
		{
			name:   "binary as keyword",
			column: schema.Column{Name: "c", Type: "blob"},
			props:  `{"c":{"type":"keyword"}}`,
			want:   "binary",
		},
		{
			name:   "enum as text",
			column: schema.Column{Name: "c", Type: "enum('a','b')"},
			props:  `{"c":{"type":"text"}}`,
			want:   "keyword",
		},
		{
			name:   "float in an integer field",
			column: schema.Column{Name: "c", Type: "float"},
			props:  `{"c":{"type":"integer"}}`,
			want:   "float",
		},
	}
	for _, tc := range cases {
		table := schema.Table{Database: "shop", Table: "t", Columns: []schema.Column{tc.column}}
		err := checkCompatibility("shop.t", table, propertiesOf(t, tc.props), tc.dynamic)
		if err == nil {
			t.Errorf("%s: expected a conflict", tc.name)
			continue
		}
		if !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: error %q does not mention %q", tc.name, err, tc.want)
		}
	}
}

func TestCheckCompatibilityRejectsMissingColumn(t *testing.T) {
	table := schema.Table{
		Database: "shop", Table: "t",
		Columns: []schema.Column{
			{Name: "id", Type: "int(11)"},
			{Name: "name", Type: "varchar(10)"},
		},
	}
	err := checkCompatibility("shop.t", table, propertiesOf(t, `{"id":{"type":"integer"}}`), "")
	if err == nil || !strings.Contains(err.Error(), `"name"`) {
		t.Fatalf("expected a missing-column error, got %v", err)
	}
}

func TestCheckCompatibilityAllowsJsonWithFieldDynamic(t *testing.T) {
	table := schema.Table{
		Database: "shop", Table: "t",
		Columns: []schema.Column{{Name: "c", Type: "json"}},
	}
	if err := checkCompatibility("shop.t", table, propertiesOf(t, `{"c":{"type":"object","dynamic":true}}`), "strict"); err != nil {
		t.Fatalf("object with dynamic true must be accepted: %v", err)
	}
}

func boolPtr(value bool) *bool { return &value }

func typesTable() schema.Table {
	return schema.Table{
		Database:    "shop",
		Table:       "types",
		PrimaryKeys: []string{"ID"},
		Columns: []schema.Column{
			{Name: "ID", Type: "bigint(20) unsigned"},
			{Name: "Flag", Type: "tinyint(1)"},
			{Name: "Amount", Type: "decimal(10,2)"},
			{Name: "Day", Type: "date"},
			{Name: "Seen", Type: "datetime(6)"},
			{Name: "Updated", Type: "timestamp"},
			{Name: "Alarm", Type: "time(3)"},
			{Name: "Name", Type: "varchar(100)"},
			{Name: "Note", Type: "text"},
			{Name: "Meta", Type: "json"},
			{Name: "Blob", Type: "blob"},
			{Name: "Status", Type: "enum('new','done')"},
			{Name: "Tags", Type: "set('a','b','c')"},
		},
	}
}

// propertiesOf decodes a JSON object of properties for compatibility tests.
func propertiesOf(t *testing.T, body string) map[string]json.RawMessage {
	t.Helper()
	props := map[string]json.RawMessage{}
	if err := json.Unmarshal([]byte(body), &props); err != nil {
		t.Fatalf("decode properties %s: %v", body, err)
	}
	return props
}
