package elasticsearch

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
)

// momentConfig is a sink config usable without a cluster: only New validates it.
func momentConfig() definition.Sink {
	return definition.Sink{Type: "elasticsearch", Hosts: []string{"http://h:9200"}}
}

// momentTable is the temporal shape of tpl01_typed: one TIMESTAMP plus the
// literal temporals that must never be shifted.
func momentTable() schema.Table {
	return schema.Table{
		Database:    "shop",
		Table:       "moments",
		PrimaryKeys: []string{"id"},
		Columns: []schema.Column{
			{Name: "id", Type: "int"},
			{Name: "c_timestamp", Type: "timestamp(6)"},
			{Name: "c_datetime", Type: "datetime(6)"},
			{Name: "c_date", Type: "date"},
			{Name: "c_year", Type: "year"},
		},
	}
}

func momentRow(timestamp string) map[string]any {
	return map[string]any{
		"id":          int64(1),
		"c_timestamp": timestamp,
		"c_datetime":  timestamp,
		"c_date":      timestamp[:10],
		"c_year":      timestamp[:4],
	}
}

func documentFields(t *testing.T, meta *tableMeta, row map[string]any) map[string]string {
	t.Helper()
	body, err := meta.document(row)
	if err != nil {
		t.Fatalf("document: %v", err)
	}
	raw := map[string]json.RawMessage{}
	if err := json.Unmarshal(body, &raw); err != nil {
		t.Fatalf("decode document: %v", err)
	}
	out := map[string]string{}
	for name, value := range raw {
		out[name] = strings.Trim(string(value), `"`)
	}
	return out
}

// Both capture paths hand over the wall clock of source.server-time-zone, so a
// snapshot row and a binlog row for the same instant must produce one value.
func TestTimestampDocumentShiftsServerZoneToUTC(t *testing.T) {
	zone := time.FixedZone("+08:00", 8*3600)
	for name, text := range map[string]string{
		"snapshot text": "2026-10-25 10:00:00.000000",
		"binlog text":   "2026-10-25 10:00:00.000000",
	} {
		meta := mustMeta(t, momentTable())
		meta.serverZone = zone
		got := documentFields(t, meta, momentRow(text))["c_timestamp"]
		want := "2026-10-25 02:00:00.000000"
		if got != want {
			t.Fatalf("%s: c_timestamp = %q, want %q", name, got, want)
		}
	}
}

// Crossing midnight backwards is the failure the load suite saw as an 8 hour drift.
func TestTimestampCrossDayKeepsMicroseconds(t *testing.T) {
	meta := mustMeta(t, momentTable())
	meta.serverZone = time.FixedZone("+08:00", 8*3600)
	got := documentFields(t, meta, momentRow("2026-10-25 00:30:00.123456"))
	if got["c_timestamp"] != "2026-10-24 16:30:00.123456" {
		t.Fatalf("cross-day shift = %q, want 2026-10-24 16:30:00.123456", got["c_timestamp"])
	}
}

// DATETIME, DATE and YEAR are literals: MySQL never converts them by session zone.
func TestLiteralTemporalColumnsAreNotConverted(t *testing.T) {
	meta := mustMeta(t, momentTable())
	meta.serverZone = time.FixedZone("+08:00", 8*3600)
	got := documentFields(t, meta, momentRow("2026-10-25 00:30:00.123456"))
	if got["c_datetime"] != "2026-10-25 00:30:00.123456" {
		t.Fatalf("c_datetime = %q, want the literal wall clock", got["c_datetime"])
	}
	if got["c_date"] != "2026-10-25" {
		t.Fatalf("c_date = %q, want 2026-10-25", got["c_date"])
	}
	if got["c_year"] != "2026" {
		t.Fatalf("c_year = %q, want 2026", got["c_year"])
	}
}

// A pipeline already running in UTC must not shift anything: the conversion
// happens exactly once, and only when the source zone carries an offset.
func TestUTCServerZoneLeavesTimestampUntouched(t *testing.T) {
	for _, zone := range []string{"", "UTC", "Z", "+00:00"} {
		s, err := New(momentConfig())
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if err := s.SetServerTimeZone(zone); err != nil {
			t.Fatalf("SetServerTimeZone(%q): %v", zone, err)
		}
		meta := mustMeta(t, momentTable())
		meta.serverZone = s.serverZone
		got := documentFields(t, meta, momentRow("2026-10-25 10:00:00.000123"))
		if got["c_timestamp"] != "2026-10-25 10:00:00.000123" {
			t.Fatalf("zone %q shifted the value to %q", zone, got["c_timestamp"])
		}
	}
}

// _id hashes the canonical wall-clock text, so enabling the UTC shift may not
// move a document whose primary key is a TIMESTAMP.
func TestTimestampKeyDigestIgnoresZoneShift(t *testing.T) {
	table := schema.Table{
		Database:    "shop",
		Table:       "bytime",
		PrimaryKeys: []string{"seen"},
		Columns:     []schema.Column{{Name: "seen", Type: "timestamp(6)"}},
	}
	row := map[string]any{"seen": "2026-10-25 10:00:00.000000"}
	plain, err := mustMeta(t, table).idFor(row)
	if err != nil {
		t.Fatalf("idFor: %v", err)
	}
	shifted := mustMeta(t, table)
	shifted.serverZone = time.FixedZone("+08:00", 8*3600)
	got, err := shifted.idFor(row)
	if err != nil {
		t.Fatalf("idFor: %v", err)
	}
	if got != plain {
		t.Fatalf("the UTC shift changed the _id: %q != %q", got, plain)
	}
}

func TestSetServerTimeZoneResolvesZone(t *testing.T) {
	s, err := New(momentConfig())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.SetServerTimeZone("Asia/Shanghai"); err != nil {
		t.Fatalf("SetServerTimeZone: %v", err)
	}
	meta := mustMeta(t, momentTable())
	meta.serverZone = s.serverZone
	got := documentFields(t, meta, momentRow("2026-10-25 10:00:00.000000"))
	if got["c_timestamp"] != "2026-10-25 02:00:00.000000" {
		t.Fatalf("Asia/Shanghai shifted to %q", got["c_timestamp"])
	}
	if err := s.SetServerTimeZone("Not/AZone"); err == nil {
		t.Fatal("an unknown zone name was accepted")
	}
	if err := s.SetServerTimeZone("+08"); err == nil {
		t.Fatal("an offset without minutes was accepted")
	}
}
