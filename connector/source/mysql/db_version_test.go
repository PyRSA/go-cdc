package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/source"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

func TestParseServerVersionFlavor(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantFlavor string
		wantKnown  bool
	}{
		{name: "mysql 5.7", raw: "5.7.42", wantFlavor: flavorMySQL, wantKnown: true},
		{name: "mysql 5.7 log suffix", raw: "5.7.42-log", wantFlavor: flavorMySQL, wantKnown: true},
		{name: "mysql 8.0 ubuntu package", raw: "8.0.36-0ubuntu0.22.04.1", wantFlavor: flavorMySQL, wantKnown: true},
		{name: "mysql 8.4", raw: "8.4.0", wantFlavor: flavorMySQL, wantKnown: true},
		{name: "mysql 8.4 patch", raw: "8.4.6", wantFlavor: flavorMySQL, wantKnown: true},
		{name: "mysql 8.4 rc", raw: "8.4.0-rc", wantFlavor: flavorMySQL, wantKnown: true},
		{name: "mysql 9.0", raw: "9.0.0", wantFlavor: flavorMySQL, wantKnown: true},
		{name: "mysql 9.6", raw: "9.6.0", wantFlavor: flavorMySQL, wantKnown: true},
		{name: "mariadb", raw: "10.11.2-MariaDB", wantFlavor: flavorMariaDB, wantKnown: true},
		{name: "mariadb lowercase", raw: "11.4.2-mariadb-log", wantFlavor: flavorMariaDB, wantKnown: true},
		{name: "empty", raw: "", wantFlavor: "", wantKnown: false},
		{name: "not a version", raw: "unknown", wantFlavor: "", wantKnown: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version := parseServerVersion(tt.raw)
			if version.flavor != tt.wantFlavor {
				t.Fatalf("flavor = %q, want %q", version.flavor, tt.wantFlavor)
			}
			if got := version.known(); got != tt.wantKnown {
				t.Fatalf("known() = %v, want %v", got, tt.wantKnown)
			}
			if version.raw != tt.raw {
				t.Fatalf("raw = %q, want the probe string verbatim %q", version.raw, tt.raw)
			}
		})
	}
}

func TestNewerReplicationTermsThreshold(t *testing.T) {
	tests := []struct {
		raw  string
		want bool
	}{
		{raw: "5.7.42", want: false},
		{raw: "8.0.36", want: false},
		{raw: "8.3.10", want: false},
		{raw: "8.4.0", want: true},
		{raw: "8.4.6", want: true},
		{raw: "9.0.0", want: true},
		// 10.11 is numerically newer than 8.4 but MariaDB never removed the
		// legacy spelling, and it does not accept the MySQL 8.4 one.
		{raw: "10.11.2-MariaDB", want: false},
		{raw: "", want: false},
		{raw: "unknown", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			if got := parseServerVersion(tt.raw).newerReplicationTerms(); got != tt.want {
				t.Fatalf("newerReplicationTerms() for %q = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}

func TestMasterStatusQueries(t *testing.T) {
	tests := []struct {
		raw          string
		wantFirst    string
		wantFallback string
	}{
		{raw: "5.7.42", wantFirst: masterStatusLegacyQuery},
		{raw: "8.0.36", wantFirst: masterStatusLegacyQuery},
		{raw: "8.3.10", wantFirst: masterStatusLegacyQuery},
		{raw: "8.4.0", wantFirst: masterStatusModernQuery},
		{raw: "8.4.6", wantFirst: masterStatusModernQuery},
		{raw: "9.0.0", wantFirst: masterStatusModernQuery},
		{raw: "10.11.2-MariaDB", wantFirst: masterStatusLegacyQuery},
		// An unparsed release must not be assumed to be 8.4+: try the modern
		// statement, then fall back once on a syntax error.
		{raw: "", wantFirst: masterStatusModernQuery, wantFallback: masterStatusLegacyQuery},
		{raw: "unknown", wantFirst: masterStatusModernQuery, wantFallback: masterStatusLegacyQuery},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			first, fallback := masterStatusQueries(parseServerVersion(tt.raw))
			if first != tt.wantFirst || fallback != tt.wantFallback {
				t.Fatalf("queries = (%q, %q), want (%q, %q)", first, fallback, tt.wantFirst, tt.wantFallback)
			}
		})
	}
}

func TestReplicasQueries(t *testing.T) {
	tests := []struct {
		raw          string
		wantFirst    string
		wantFallback string
	}{
		{raw: "5.7.42", wantFirst: replicasLegacyQuery},
		{raw: "8.0.36", wantFirst: replicasLegacyQuery},
		{raw: "8.4.0", wantFirst: replicasModernQuery},
		{raw: "9.0.0", wantFirst: replicasModernQuery},
		{raw: "10.11.2-MariaDB", wantFirst: replicasLegacyQuery},
		{raw: "", wantFirst: replicasModernQuery, wantFallback: replicasLegacyQuery},
	}
	for _, tt := range tests {
		t.Run(tt.raw, func(t *testing.T) {
			first, fallback := replicasQueries(parseServerVersion(tt.raw))
			if first != tt.wantFirst || fallback != tt.wantFallback {
				t.Fatalf("queries = (%q, %q), want (%q, %q)", first, fallback, tt.wantFirst, tt.wantFallback)
			}
		})
	}
}

func TestIsSyntaxError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "syntax error", err: &mysqldriver.MySQLError{Number: 1064, Message: "check the manual"}, want: true},
		{name: "wrapped syntax error", err: fmt.Errorf("query: %w", &mysqldriver.MySQLError{Number: 1064}), want: true},
		{name: "missing table", err: &mysqldriver.MySQLError{Number: 1146}, want: false},
		{name: "access denied", err: &mysqldriver.MySQLError{Number: 1142}, want: false},
		{name: "plain error", err: errors.New("connection reset"), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSyntaxError(tt.err); got != tt.want {
				t.Fatalf("isSyntaxError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestParseMasterStatusColumns(t *testing.T) {
	t.Run("legacy and modern column sets", func(t *testing.T) {
		// SHOW MASTER STATUS and SHOW BINARY LOG STATUS expose the same three
		// columns; parsing is keyed on names so both spellings land here.
		cols := []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
		raw := []sql.NullString{
			{String: "mysql-bin.000042", Valid: true},
			{String: "1234", Valid: true},
			{String: "", Valid: true},
			{String: "", Valid: true},
			{String: "3e11f4c2-11b9-11ee-9c4e-0242ac120002:1-42", Valid: true},
		}
		pos, err := parseMasterStatus(cols, raw)
		if err != nil {
			t.Fatalf("parseMasterStatus: %v", err)
		}
		want := checkpoint.Position{File: "mysql-bin.000042", Pos: 1234, GTID: "3e11f4c2-11b9-11ee-9c4e-0242ac120002:1-42"}
		if pos != want {
			t.Fatalf("position = %+v, want %+v", pos, want)
		}
	})

	t.Run("null gtid set", func(t *testing.T) {
		cols := []string{"File", "Position", "Executed_Gtid_Set"}
		raw := []sql.NullString{
			{String: "mysql-bin.000007", Valid: true},
			{String: "4", Valid: true},
			{Valid: false},
		}
		pos, err := parseMasterStatus(cols, raw)
		if err != nil {
			t.Fatalf("parseMasterStatus: %v", err)
		}
		if pos.GTID != "" || pos.File != "mysql-bin.000007" || pos.Pos != 4 {
			t.Fatalf("position = %+v, want file/pos with an empty gtid", pos)
		}
	})
}

func TestParseMasterStatusRejectsBrokenRows(t *testing.T) {
	tests := []struct {
		name string
		cols []string
		raw  []sql.NullString
	}{
		{
			name: "missing position column",
			cols: []string{"File"},
			raw:  []sql.NullString{{String: "mysql-bin.000001", Valid: true}},
		},
		{
			name: "missing file column",
			cols: []string{"Position"},
			raw:  []sql.NullString{{String: "4", Valid: true}},
		},
		{
			name: "empty file",
			cols: []string{"File", "Position"},
			raw:  []sql.NullString{{Valid: true}, {String: "4", Valid: true}},
		},
		{
			name: "null position",
			cols: []string{"File", "Position"},
			raw:  []sql.NullString{{String: "mysql-bin.000001", Valid: true}, {Valid: false}},
		},
		{
			name: "position overflows uint32",
			cols: []string{"File", "Position"},
			raw:  []sql.NullString{{String: "mysql-bin.000001", Valid: true}, {String: "4294967296", Valid: true}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseMasterStatus(tt.cols, tt.raw); err == nil {
				t.Fatalf("parseMasterStatus(%v, %v) = nil error, want a failure", tt.cols, tt.raw)
			}
		})
	}
}

func TestBinlogModeRequiresBothVariables(t *testing.T) {
	tests := []struct {
		name       string
		values     map[string]string
		queryErr   error
		wantFormat string
		wantImage  string
		wantErr    bool
	}{
		{
			name:       "row and full",
			values:     map[string]string{"binlog_format": "ROW", "binlog_row_image": "FULL"},
			wantFormat: "ROW",
			wantImage:  "FULL",
		},
		{
			// The gate is a separate step: a reported STATEMENT must survive the
			// read so requireRowAndFull can reject it instead of being rewritten.
			name:       "statement is reported verbatim",
			values:     map[string]string{"binlog_format": "STATEMENT", "binlog_row_image": "FULL"},
			wantFormat: "STATEMENT",
			wantImage:  "FULL",
		},
		{
			name:       "mixed is reported verbatim",
			values:     map[string]string{"binlog_format": "MIXED", "binlog_row_image": "FULL"},
			wantFormat: "MIXED",
			wantImage:  "FULL",
		},
		{
			// binlog_format is deprecated, not removed, so an absent row is a
			// failed read and must never turn into ROW.
			name:    "binlog_format absent",
			values:  map[string]string{"binlog_row_image": "FULL"},
			wantErr: true,
		},
		{
			name:    "binlog_row_image absent",
			values:  map[string]string{"binlog_format": "ROW"},
			wantErr: true,
		},
		{
			name:    "empty result set",
			values:  map[string]string{},
			wantErr: true,
		},
		{
			// A privilege failure must surface as itself, not as a missing
			// variable that a newer release supposedly dropped.
			name:     "access denied",
			queryErr: &mysqldriver.MySQLError{Number: 1227, Message: "Access denied; you need the SUPER privilege"},
			wantErr:  true,
		},
		{
			name:     "connection failure",
			queryErr: errors.New("connection reset by peer"),
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			format, image, err := binlogMode(tt.values, tt.queryErr)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("binlogMode(%v, %v) = (%q, %q), want an error", tt.values, tt.queryErr, format, image)
				}
				if tt.queryErr != nil && !errors.Is(err, tt.queryErr) {
					t.Fatalf("binlogMode error %v does not wrap the query failure %v", err, tt.queryErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("binlogMode: %v", err)
			}
			if format != tt.wantFormat || image != tt.wantImage {
				t.Fatalf("binlogMode = (%q, %q), want (%q, %q)", format, image, tt.wantFormat, tt.wantImage)
			}
		})
	}
}

func TestRequireRowAndFull(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		image   string
		raw     string
		wantErr bool
	}{
		{name: "mysql 5.7 row full", format: "ROW", image: "FULL", raw: "5.7.42"},
		{name: "mysql 8.0 row full", format: "ROW", image: "FULL", raw: "8.0.36"},
		{name: "mysql 8.4 row full", format: "ROW", image: "FULL", raw: "8.4.11"},
		{name: "mysql 9.x row full", format: "ROW", image: "FULL", raw: "9.6.0"},
		{name: "case insensitive", format: "row", image: "full", raw: "8.4.11"},
		{name: "statement", format: "STATEMENT", image: "FULL", raw: "8.4.11", wantErr: true},
		{name: "mixed", format: "MIXED", image: "FULL", raw: "9.0.0", wantErr: true},
		{name: "minimal row image", format: "ROW", image: "MINIMAL", raw: "8.0.36", wantErr: true},
		{name: "both empty", format: "", image: "", raw: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := requireRowAndFull(tt.format, tt.image, parseServerVersion(tt.raw))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("requireRowAndFull(%q, %q) = nil, want an error", tt.format, tt.image)
				}
				if !strings.Contains(err.Error(), serverVersionLabel(parseServerVersion(tt.raw))) {
					t.Fatalf("error %q does not name the source release", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("requireRowAndFull(%q, %q): %v", tt.format, tt.image, err)
			}
		})
	}
}

func TestRetryFailedKeepsBothAttempts(t *testing.T) {
	// Two distinct failures, so dropping either %w in retryFailed fails here.
	firstErr := &mysqldriver.MySQLError{Number: 1064, Message: "you have an error in your SQL syntax"}
	fallbackErr := &mysqldriver.MySQLError{Number: 1064, Message: "check the manual"}
	err := retryFailed(masterStatusModernQuery, firstErr, masterStatusLegacyQuery, fallbackErr)
	for _, want := range []string{masterStatusModernQuery, masterStatusLegacyQuery} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
	// Both failures stay in the chain, so a caller can still tell a syntax error
	// apart from a privilege or connection failure on either statement.
	if !errors.Is(err, firstErr) {
		t.Fatalf("error %q does not wrap the first statement failure", err)
	}
	if !errors.Is(err, fallbackErr) {
		t.Fatalf("error %q does not wrap the retry failure", err)
	}
	var myErr *mysqldriver.MySQLError
	if !errors.As(err, &myErr) || myErr.Message != firstErr.Message {
		t.Fatalf("error %q does not expose the first statement failure through errors.As, got %#v", err, myErr)
	}
}

func TestRejectUnsupportedColumnsIsSentinel(t *testing.T) {
	selector, err := definition.CompileSelector(`shop\..*`, "")
	if err != nil {
		t.Fatalf("CompileSelector: %v", err)
	}
	db := &sqlDB{cfg: definition.Source{Tables: selector}}
	err = db.rejectUnsupportedColumns([]schema.Table{{
		Database: "shop", Table: "items",
		Columns: []schema.Column{
			{Name: "id", Type: "bigint"},
			{Name: "embedding", Type: "vector(1536)"},
		},
	}}, false)
	if err == nil {
		t.Fatal("a captured table with a VECTOR column must be rejected")
	}
	if !errors.Is(err, ErrUnsupportedColumn) {
		t.Fatalf("error %v is not ErrUnsupportedColumn; the capture cannot tell it apart from a recoverable catalogue failure", err)
	}
	for _, want := range []string{"shop.items.embedding", "vector(1536)", "source.tables.exclude"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not mention %q", err, want)
		}
	}
}

// warnRecorder collects the fallback notices so a test can prove a VECTOR
// rejection never travels through them.
type warnRecorder struct{ calls []string }

func (w *warnRecorder) log(msg string, _ ...any) { w.calls = append(w.calls, msg) }

// newRuntimeCreateCapture wires a Capture for the runtime CREATE path: one
// commit carrying both the binlog-derived schema and the CREATE DDL, with no
// spill (a rejected CREATE must never reach the spill loop).
func newRuntimeCreateCapture(t *testing.T) (*Capture, chan source.Message, *memDB, *warnRecorder) {
	t.Helper()
	db := newMemDB(nil)
	stream := &memStream{events: []Commit{{
		Schema: &schema.Table{Database: "db", Table: "items", Columns: []schema.Column{
			{Name: "id", Type: "bigint"},
			{Name: "embedding", Type: "vector(1536)"},
		}},
		Event: &event.Event{
			Op: event.OpDDL, Database: "db", Table: "items",
			SourceDatabase: "db", SourceTable: "items",
			DDL: "CREATE TABLE items (id bigint, embedding vector(1536))",
		},
	}}}
	warns := &warnRecorder{}
	capture := &Capture{
		Config: definition.Source{BinlogNewlyAddedTable: true, SchemaChange: true},
		DB:     db,
		Stream: stream,
		Warn:   warns.log,
	}
	return capture, make(chan source.Message, 8), db, warns
}

func TestBinlogCreateUnsupportedColumnStopsTheJob(t *testing.T) {
	capture, out, db, warns := newRuntimeCreateCapture(t)
	db.describeErr = fmt.Errorf("%w: db.items.embedding has type vector(1536)", ErrUnsupportedColumn)

	err := capture.binlogFrom(context.Background(), out, checkpoint.Position{File: "mysql-bin.000001", Pos: 4}, map[string]bool{})
	if !errors.Is(err, ErrUnsupportedColumn) {
		t.Fatalf("binlogFrom = %v, want ErrUnsupportedColumn", err)
	}
	if len(out) != 0 {
		t.Fatalf("a rejected CREATE must not reach the sink, got %d queued messages", len(out))
	}
	if len(warns.calls) != 0 {
		t.Fatalf("a VECTOR rejection must not be downgraded to a warning, got %q", warns.calls)
	}
}

func TestBinlogCreateUnsupportedColumnKeepsFallbackForOtherErrors(t *testing.T) {
	capture, out, db, warns := newRuntimeCreateCapture(t)
	db.describeErr = errors.New("driver: bad connection")

	if err := capture.binlogFrom(context.Background(), out, checkpoint.Position{File: "mysql-bin.000001", Pos: 4}, map[string]bool{}); err != nil {
		t.Fatalf("an ordinary describe failure must stay recoverable, got %v", err)
	}
	if len(warns.calls) != 1 {
		t.Fatalf("want exactly one fallback warning, got %q", warns.calls)
	}
	if len(out) != 1 {
		t.Fatalf("want the CREATE event with its schema, got %d messages", len(out))
	}
	msg := <-out
	if msg.Schema == nil {
		t.Fatal("the CREATE event must carry a schema message")
	}
	found := false
	for _, column := range msg.Schema.Columns {
		if column.Name == "embedding" && column.Type == "vector(1536)" {
			found = true
		}
	}
	if !found {
		t.Fatalf("fallback must keep the binlog schema verbatim, got %#v", msg.Schema.Columns)
	}
}
