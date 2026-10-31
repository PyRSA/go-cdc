package elasticsearch

import (
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/connector/sink/elasticsearch/client"
)

func TestIDForSingleKeyIsStable(t *testing.T) {
	meta, err := newTableMeta(typesTable(), "shop.types")
	if err != nil {
		t.Fatalf("newTableMeta: %v", err)
	}
	first, err := meta.idFor(typesRow(7))
	if err != nil {
		t.Fatalf("idFor: %v", err)
	}
	second, err := meta.idFor(typesRow(7))
	if err != nil {
		t.Fatalf("idFor: %v", err)
	}
	if first != second {
		t.Fatalf("id is not stable: %q != %q", first, second)
	}
	if len(first) != 64 {
		t.Fatalf("id %q is not a SHA-256 hex digest", first)
	}
}

func TestIDIgnoresNonKeyColumns(t *testing.T) {
	meta, err := newTableMeta(typesTable(), "shop.types")
	if err != nil {
		t.Fatalf("newTableMeta: %v", err)
	}
	first, err := meta.idFor(typesRow(7))
	if err != nil {
		t.Fatalf("idFor: %v", err)
	}
	changed := typesRow(7)
	changed["Name"] = "another widget"
	second, err := meta.idFor(changed)
	if err != nil {
		t.Fatalf("idFor: %v", err)
	}
	if first != second {
		t.Fatal("a non-key column changed the _id")
	}
}

func TestIDCompositeKeyIsUnambiguous(t *testing.T) {
	table := schema.Table{
		Database: "shop", Table: "pairs", PrimaryKeys: []string{"a", "b"},
		Columns: []schema.Column{
			{Name: "a", Type: "varchar(10)"},
			{Name: "b", Type: "varchar(10)"},
		},
	}
	meta, err := newTableMeta(table, "shop.pairs")
	if err != nil {
		t.Fatalf("newTableMeta: %v", err)
	}
	cases := [][2]string{{"ab", "c"}, {"a", "bc"}, {"x|y", "z"}, {"x", "y|z"}}
	seen := map[string]string{}
	for _, values := range cases {
		id, err := meta.idFor(map[string]any{"a": values[0], "b": values[1]})
		if err != nil {
			t.Fatalf("idFor(%v): %v", values, err)
		}
		label := values[0] + "/" + values[1]
		if other, ok := seen[id]; ok {
			t.Fatalf("%s and %s share _id %q", other, label, id)
		}
		seen[id] = label
	}
}

func TestIDKeyOrderChangesTheDigest(t *testing.T) {
	forward := schema.Table{
		Database: "shop", Table: "pairs", PrimaryKeys: []string{"a", "b"},
		Columns: []schema.Column{{Name: "a", Type: "varchar(10)"}, {Name: "b", Type: "varchar(10)"}},
	}
	reverse := forward
	reverse.PrimaryKeys = []string{"b", "a"}
	row := map[string]any{"a": "1", "b": "2"}

	first, err := mustMeta(t, forward).idFor(row)
	if err != nil {
		t.Fatalf("idFor: %v", err)
	}
	second, err := mustMeta(t, reverse).idFor(row)
	if err != nil {
		t.Fatalf("idFor: %v", err)
	}
	if first == second {
		t.Fatal("primary key order did not change the _id")
	}
}

func TestIDUnicodeAndCaseInsensitiveLookup(t *testing.T) {
	meta := mustMeta(t, schema.Table{
		Database: "shop", Table: "people", PrimaryKeys: []string{"Name"},
		Columns: []schema.Column{{Name: "Name", Type: "varchar(64)"}},
	})
	lower, err := meta.idFor(map[string]any{"Name": "José"})
	if err != nil {
		t.Fatalf("idFor: %v", err)
	}
	upper, err := meta.idFor(map[string]any{"NAME": "José"})
	if err != nil {
		t.Fatalf("idFor: %v", err)
	}
	if lower != upper {
		t.Fatal("the same Unicode key produced different ids for different map key case")
	}
}

func TestIDRejectsMissingOrNullKey(t *testing.T) {
	meta := mustMeta(t, typesTable())
	row := typesRow(7)
	row["ID"] = nil
	if _, err := meta.idFor(row); err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Fatalf("error = %v, want a NULL key failure", err)
	}
	delete(row, "ID")
	if _, err := meta.idFor(row); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("error = %v, want a missing key failure", err)
	}
}

func TestIDAcceptsEquivalentNumericForms(t *testing.T) {
	meta := mustMeta(t, typesTable())
	first, err := meta.idFor(typesRow(7))
	if err != nil {
		t.Fatalf("idFor: %v", err)
	}
	for _, value := range []any{int64(7), "7", uint64(7)} {
		row := typesRow(0)
		row["ID"] = value
		got, err := meta.idFor(row)
		if err != nil {
			t.Fatalf("idFor(%T): %v", value, err)
		}
		if got != first {
			t.Fatalf("key form %T changed the _id", value)
		}
	}
}

func TestIDCanonicalizesDecimalKey(t *testing.T) {
	meta := mustMeta(t, schema.Table{
		Database: "shop", Table: "money", PrimaryKeys: []string{"amount"},
		Columns: []schema.Column{{Name: "amount", Type: "decimal(10,2)"}},
	})
	first, err := meta.idFor(map[string]any{"amount": "1.5"})
	if err != nil {
		t.Fatalf("idFor: %v", err)
	}
	for _, value := range []any{"1.50", "1.500", []byte("1.50")} {
		got, err := meta.idFor(map[string]any{"amount": value})
		if err != nil {
			t.Fatalf("idFor(%v): %v", value, err)
		}
		if got != first {
			t.Fatalf("decimal form %v changed the _id", value)
		}
	}
}

func TestIDCanonicalizesDatetimeKey(t *testing.T) {
	meta := mustMeta(t, schema.Table{
		Database: "shop", Table: "events", PrimaryKeys: []string{"seen"},
		Columns: []schema.Column{{Name: "seen", Type: "datetime(6)"}},
	})
	first, err := meta.idFor(map[string]any{"seen": "2024-01-02 03:04:05.000000"})
	if err != nil {
		t.Fatalf("idFor: %v", err)
	}
	for _, value := range []any{"2024-01-02T03:04:05Z", "2024-01-02 03:04:05"} {
		got, err := meta.idFor(map[string]any{"seen": value})
		if err != nil {
			t.Fatalf("idFor(%v): %v", value, err)
		}
		if got != first {
			t.Fatalf("datetime form %v changed the _id", value)
		}
	}
}

func TestDocumentEncodesCanonicalValues(t *testing.T) {
	meta := mustMeta(t, typesTable())
	doc, err := meta.document(typesRow(7))
	if err != nil {
		t.Fatalf("document: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(doc, &fields); err != nil {
		t.Fatalf("decode document: %v", err)
	}
	want := map[string]string{
		"ID":      `7`,
		"Flag":    `1`,
		"Amount":  `"10.50"`,
		"Day":     `"2024-01-02"`,
		"Seen":    `"2024-01-02 03:04:05.000000"`,
		"Updated": `"2024-01-02 03:04:05.000000"`,
		"Alarm":   `"12:34:56.000000"`,
		"Name":    `"widget"`,
		"Note":    `"note"`,
		"Meta":    `{"b":2,"a":1}`,
		"Blob":    `"aGk="`,
		"Status":  `"done"`,
		"Tags":    `"a,c"`,
	}
	for name, expected := range want {
		if got := string(fields[name]); got != expected {
			t.Errorf("field %s = %s, want %s", name, got, expected)
		}
	}
}

func TestDocumentEncodesNull(t *testing.T) {
	meta := mustMeta(t, typesTable())
	row := typesRow(7)
	row["Note"] = nil
	doc, err := meta.document(row)
	if err != nil {
		t.Fatalf("document: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(doc, &fields); err != nil {
		t.Fatalf("decode document: %v", err)
	}
	if got := string(fields["Note"]); got != "null" {
		t.Fatalf("Note = %s, want null", got)
	}
}

func TestDocumentRejectsMissingColumn(t *testing.T) {
	meta := mustMeta(t, typesTable())
	row := typesRow(7)
	delete(row, "Note")
	_, err := meta.document(row)
	if err == nil || !strings.Contains(err.Error(), `"Note"`) {
		t.Fatalf("error = %v, want the missing column", err)
	}
}

func TestDocumentRejectsNaN(t *testing.T) {
	meta := mustMeta(t, schema.Table{
		Database: "shop", Table: "floats", PrimaryKeys: []string{"v"},
		Columns: []schema.Column{{Name: "v", Type: "double"}},
	})
	_, err := meta.document(map[string]any{"v": math.NaN()})
	if err == nil || !strings.Contains(err.Error(), "JSON") {
		t.Fatalf("error = %v, want a JSON representability failure", err)
	}
}

func TestCanonicalDecimal(t *testing.T) {
	cases := []struct {
		text  string
		scale int
		want  string
		bad   bool
	}{
		{text: "1.5", scale: 2, want: "1.50"},
		{text: "1.500", scale: 2, want: "1.50"},
		{text: "001.20", scale: 2, want: "1.20"},
		{text: "-0.00", scale: 2, want: "0.00"},
		{text: "1.505", scale: 2, bad: true},
		{text: "", scale: 2, bad: true},
		{text: "abc", scale: 2, bad: true},
	}
	for _, tc := range cases {
		got, err := canonicalDecimal(tc.text, tc.scale)
		if tc.bad {
			if err == nil {
				t.Errorf("canonicalDecimal(%q): expected an error", tc.text)
			}
			continue
		}
		if err != nil {
			t.Errorf("canonicalDecimal(%q): %v", tc.text, err)
			continue
		}
		if got != tc.want {
			t.Errorf("canonicalDecimal(%q) = %q, want %q", tc.text, got, tc.want)
		}
	}
}

func TestCanonicalTime(t *testing.T) {
	cases := []struct {
		text string
		want string
		bad  bool
	}{
		{text: "12:34:56", want: "12:34:56.000000"},
		{text: "-12:34:56.5", want: "-12:34:56.500000"},
		{text: "838:59:59", want: "838:59:59.000000"},
		{text: "-00:00:00.000", want: "0:00:00.000000"},
		{text: "839:00:00", bad: true},
		{text: "12:60:00", bad: true},
		{text: "12:00", bad: true},
		{text: "", bad: true},
	}
	for _, tc := range cases {
		got, err := canonicalTime(tc.text)
		if tc.bad {
			if err == nil {
				t.Errorf("canonicalTime(%q): expected an error", tc.text)
			}
			continue
		}
		if err != nil {
			t.Errorf("canonicalTime(%q): %v", tc.text, err)
			continue
		}
		if got != tc.want {
			t.Errorf("canonicalTime(%q) = %q, want %q", tc.text, got, tc.want)
		}
	}
}

func mustColumnType(t *testing.T, raw string) columnType {
	t.Helper()
	ct, err := parseColumnType(raw)
	if err != nil {
		t.Fatalf("parseColumnType(%q): %v", raw, err)
	}
	return ct
}

func TestCanonicalEnumSet(t *testing.T) {
	enum := mustColumnType(t, "enum('new','done')")
	numEnum := mustColumnType(t, "enum('2','1')")
	set := mustColumnType(t, "set('a','b','c')")
	numSet := mustColumnType(t, "set('1','2')")

	// Label form: the snapshot path (string/[]byte). A numeric-looking string
	// stays a label and must never be re-read as an index or bitmask.
	labelCases := []struct {
		ct   columnType
		text string
		want string
		bad  bool
	}{
		{enum, "done", "done", false},
		{enum, "new", "new", false},
		{enum, "", "", false},
		{enum, "bogus", "", true},
		{numEnum, "2", "2", false},
		{numEnum, "1", "1", false},
		{set, "a,c", "a,c", false},
		{set, "", "", false},
		{set, "z", "", true},
		{numSet, "1", "1", false},
	}
	for _, tc := range labelCases {
		got, err := canonicalEnumSet(tc.ct, tc.text)
		if tc.bad {
			if err == nil {
				t.Errorf("canonicalEnumSet(%s, %q): expected an error", tc.ct.Raw, tc.text)
			}
			continue
		}
		if err != nil {
			t.Errorf("canonicalEnumSet(%s, %q): %v", tc.ct.Raw, tc.text, err)
			continue
		}
		if got != tc.want {
			t.Errorf("canonicalEnumSet(%s, %q) = %q, want %q", tc.ct.Raw, tc.text, got, tc.want)
		}
	}

	// Numeric form: the binlog path (int64 index for ENUM, bitmask for SET).
	numericCases := []struct {
		ct   columnType
		code any
		want string
		bad  bool
	}{
		{enum, int64(2), "done", false},
		{enum, int64(1), "new", false},
		{enum, int64(0), "", false},
		{enum, int64(3), "", true},
		{numEnum, int64(2), "1", false},
		{set, int64(5), "a,c", false},
		{set, int64(7), "a,b,c", false},
		{set, int64(0), "", false},
		{set, int64(8), "", true},
		{numSet, int64(3), "1,2", false},
	}
	for _, tc := range numericCases {
		got, err := canonicalEnumSet(tc.ct, tc.code)
		if tc.bad {
			if err == nil {
				t.Errorf("canonicalEnumSet(%s, %v): expected an error", tc.ct.Raw, tc.code)
			}
			continue
		}
		if err != nil {
			t.Errorf("canonicalEnumSet(%s, %v): %v", tc.ct.Raw, tc.code, err)
			continue
		}
		if got != tc.want {
			t.Errorf("canonicalEnumSet(%s, %v) = %q, want %q", tc.ct.Raw, tc.code, got, tc.want)
		}
	}
}

func TestCanonicalYear(t *testing.T) {
	ct := mustColumnType(t, "year")
	if got, err := canonicalYear(ct, "2024"); err != nil || got != "2024" {
		t.Errorf("year text: got %q, %v", got, err)
	}
	if got, err := canonicalYear(ct, int64(2024)); err != nil || got != "2024" {
		t.Errorf("year int64: got %q, %v", got, err)
	}
	if got, err := canonicalYear(ct, int64(0)); err != nil || got != "0000" {
		t.Errorf("year zero: got %q, %v", got, err)
	}
	if _, err := canonicalYear(ct, int64(20000)); err == nil {
		t.Error("out of range year: expected an error")
	}
	if _, err := canonicalYear(ct, "24"); err == nil {
		t.Error("invalid year text: expected an error")
	}
}

func TestIDEnumSetYearKeyStableAcrossPaths(t *testing.T) {
	meta := mustMeta(t, schema.Table{
		Database: "shop", Table: "flags", PrimaryKeys: []string{"status", "tags", "born"},
		Columns: []schema.Column{
			{Name: "status", Type: "enum('new','done')"},
			{Name: "tags", Type: "set('a','b','c')"},
			{Name: "born", Type: "year"},
		},
	})
	snapshot, err := meta.idFor(map[string]any{"status": "done", "tags": "a,c", "born": "2024"})
	if err != nil {
		t.Fatalf("idFor snapshot: %v", err)
	}
	binlog, err := meta.idFor(map[string]any{"status": int64(2), "tags": int64(5), "born": int64(2024)})
	if err != nil {
		t.Fatalf("idFor binlog: %v", err)
	}
	if snapshot != binlog {
		t.Fatalf("snapshot and binlog _id differ for the same row: %q != %q", snapshot, binlog)
	}

	// ENUM index 0 (blank) must agree as well: the snapshot yields "", the
	// binlog yields int64(0).
	emptySnapshot, err := meta.idFor(map[string]any{"status": "", "tags": "a,c", "born": "2024"})
	if err != nil {
		t.Fatalf("idFor blank enum snapshot: %v", err)
	}
	emptyBinlog, err := meta.idFor(map[string]any{"status": int64(0), "tags": int64(5), "born": int64(2024)})
	if err != nil {
		t.Fatalf("idFor blank enum binlog: %v", err)
	}
	if emptySnapshot != emptyBinlog {
		t.Fatalf("snapshot and binlog _id differ for a blank ENUM: %q != %q", emptySnapshot, emptyBinlog)
	}
}

func TestCanonicalDate(t *testing.T) {
	date, err := parseColumnType("date")
	if err != nil {
		t.Fatalf("parseColumnType: %v", err)
	}
	if got, err := canonicalDate(date, "2024-01-02"); err != nil || got != "2024-01-02" {
		t.Errorf("date: got %q, %v", got, err)
	}
	if _, err := canonicalDate(date, "2024-1-2"); err == nil {
		t.Error("unpadded date: expected an error")
	}
	if _, err := canonicalDate(date, "2024-13-02"); err == nil {
		t.Error("invalid date: expected an error")
	}

	year, err := parseColumnType("year")
	if err != nil {
		t.Fatalf("parseColumnType: %v", err)
	}
	if got, err := canonicalDate(year, "2024"); err != nil || got != "2024" {
		t.Errorf("year: got %q, %v", got, err)
	}
	if _, err := canonicalDate(year, "24"); err == nil {
		t.Error("invalid year: expected an error")
	}

	datetime, err := parseColumnType("timestamp")
	if err != nil {
		t.Fatalf("parseColumnType: %v", err)
	}
	if _, err := canonicalDate(datetime, "0000-00-00 00:00:00"); err == nil {
		t.Error("zero datetime: expected an error")
	}
}

func TestOperationForMapsOps(t *testing.T) {
	s, _ := registeredSink(t, typesTable())
	for _, op := range []event.Op{event.OpRead, event.OpCreate, event.OpUpdate} {
		got, err := s.operationFor(typesEvent(op, 1))
		if err != nil {
			t.Fatalf("%s: %v", op, err)
		}
		if got.Action != client.ActionIndex || got.Index != "shop.types" || len(got.Document) == 0 {
			t.Fatalf("%s: op = %+v", op, got)
		}
	}
	deleted, err := s.operationFor(typesEvent(event.OpDelete, 1))
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if deleted.Action != client.ActionDelete || len(deleted.Document) != 0 {
		t.Fatalf("delete op = %+v", deleted)
	}
}

func TestOperationForRejectsUnsupportedOps(t *testing.T) {
	s, _ := registeredSink(t, typesTable())
	ev := typesEvent(event.OpDDL, 1)
	ev.DDL = "ALTER TABLE types ADD COLUMN x INT"
	_, err := s.operationFor(ev)
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("error = %v, want an unsupported operation failure", err)
	}
	if !strings.Contains(err.Error(), "src.types") || !strings.Contains(err.Error(), "shop.types") {
		t.Fatalf("error %q must name the source and sink table", err)
	}
}

func TestOperationForRejectsMissingImages(t *testing.T) {
	s, _ := registeredSink(t, typesTable())
	create := event.Event{Op: event.OpCreate, Database: "shop", Table: "types"}
	if _, err := s.operationFor(create); err == nil || !strings.Contains(err.Error(), "After") {
		t.Fatalf("error = %v, want a missing After failure", err)
	}
	remove := event.Event{Op: event.OpDelete, Database: "shop", Table: "types"}
	if _, err := s.operationFor(remove); err == nil || !strings.Contains(err.Error(), "Before") {
		t.Fatalf("error = %v, want a missing Before failure", err)
	}
}

func TestOperationForValidatesEventKeys(t *testing.T) {
	s, _ := registeredSink(t, typesTable())
	ev := typesEvent(event.OpCreate, 1)
	ev.Keys = []string{"ID"}
	if _, err := s.operationFor(ev); err != nil {
		t.Fatalf("matching keys: %v", err)
	}
	ev.Keys = []string{"id"}
	if _, err := s.operationFor(ev); err != nil {
		t.Fatalf("case-insensitive keys: %v", err)
	}
	ev.Keys = []string{"Other"}
	if _, err := s.operationFor(ev); err == nil || !strings.Contains(err.Error(), "event key") {
		t.Fatalf("error = %v, want a key membership failure", err)
	}
	ev.Keys = []string{"ID", "Other"}
	if _, err := s.operationFor(ev); err == nil || !strings.Contains(err.Error(), "disagree") {
		t.Fatalf("error = %v, want a key count failure", err)
	}
}

func mustMeta(t *testing.T, table schema.Table) *tableMeta {
	t.Helper()
	meta, err := newTableMeta(table, indexNameFor("", table.Database, table.Table))
	if err != nil {
		t.Fatalf("newTableMeta: %v", err)
	}
	return meta
}
