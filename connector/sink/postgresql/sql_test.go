package postgresql

import (
	"strings"
	"testing"

	"github.com/PyRSA/go-cdc/common/event"
)

func TestResolveTarget(t *testing.T) {
	tests := []struct {
		name    string
		schema  string
		item    event.Event
		want    Target
		wantErr bool
	}{
		{
			name:   "unrouted uses configured schema",
			schema: "public",
			item:   event.Event{Database: "app", Table: "users", SourceDatabase: "app", SourceTable: "users"},
			want:   Target{Schema: "public", Table: "users"},
		},
		{
			name:   "routed database names the schema",
			schema: "public",
			item:   event.Event{Database: "sales", Table: "users", SourceDatabase: "app", SourceTable: "users", Routed: true},
			want:   Target{Schema: "sales", Table: "users"},
		},
		{
			name:   "table only rename keeps the routed schema",
			schema: "ods",
			item:   event.Event{Database: "app", Table: "users_v2", SourceDatabase: "app", SourceTable: "users", Routed: true},
			want:   Target{Schema: "app", Table: "users_v2"},
		},
		{
			name:   "identity route names the schema",
			schema: "public",
			item:   event.Event{Database: "app", Table: "users", SourceDatabase: "app", SourceTable: "users", Routed: true},
			want:   Target{Schema: "app", Table: "users"},
		},
		{
			name:   "empty database falls back to configured schema",
			schema: "public",
			item:   event.Event{Table: "users"},
			want:   Target{Schema: "public", Table: "users"},
		},
		{
			name:   "routed event without a database falls back to configured schema",
			schema: "public",
			item:   event.Event{Table: "users", Routed: true},
			want:   Target{Schema: "public", Table: "users"},
		},
		{
			name:    "missing table",
			schema:  "public",
			item:    event.Event{Database: "app", SourceDatabase: "app", SourceTable: "users"},
			wantErr: true,
		},
		{
			name:    "three part sink table",
			schema:  "public",
			item:    event.Event{Database: "app", Table: "a.b", SourceDatabase: "app", SourceTable: "users"},
			wantErr: true,
		},
		{
			name:    "empty schema",
			item:    event.Event{Table: "users"},
			schema:  "",
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveTarget(tc.schema, tc.item)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %#v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("got %#v want %#v", got, tc.want)
			}
		})
	}
}

func createEvent() event.Event {
	return event.Event{
		Op:             event.OpCreate,
		Database:       "public",
		Table:          "users",
		SourceDatabase: "app",
		SourceTable:    "users",
		Routed:         true,
		Columns:        []string{"id", "name"},
		Keys:           []string{"id"},
		After:          map[string]any{"id": int64(1), "name": "alice"},
	}
}

func TestStatementsUpsert(t *testing.T) {
	builder := NewBuilder("public")
	got, err := builder.Statements(createEvent())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("statements %d", len(got))
	}
	want := `INSERT INTO "public"."users" ("id", "name") VALUES ($1, $2) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name"`
	if got[0].SQL != want {
		t.Fatalf("sql:\n got %s\nwant %s", got[0].SQL, want)
	}
	if len(got[0].Args) != 2 || got[0].Args[0] != int64(1) || got[0].Args[1] != "alice" {
		t.Fatalf("args %#v", got[0].Args)
	}
	if got[0].Op != event.OpCreate || got[0].Table != (Target{Schema: "public", Table: "users"}) {
		t.Fatalf("metadata %#v", got[0])
	}
}

func TestStatementsCompositeKey(t *testing.T) {
	item := createEvent()
	item.Columns = []string{"a", "b", "c"}
	item.Keys = []string{"a", "b"}
	item.After = map[string]any{"a": int64(1), "b": int64(2), "c": "x"}
	got, err := NewBuilder("public").Statements(item)
	if err != nil {
		t.Fatal(err)
	}
	want := `INSERT INTO "public"."users" ("a", "b", "c") VALUES ($1, $2, $3) ON CONFLICT ("a", "b") DO UPDATE SET "c" = EXCLUDED."c"`
	if got[0].SQL != want {
		t.Fatalf("sql:\n got %s\nwant %s", got[0].SQL, want)
	}
}

func TestStatementsKeyOnlyTableUsesDoNothing(t *testing.T) {
	item := createEvent()
	item.Columns = []string{"id"}
	item.After = map[string]any{"id": int64(7)}
	got, err := NewBuilder("public").Statements(item)
	if err != nil {
		t.Fatal(err)
	}
	want := `INSERT INTO "public"."users" ("id") VALUES ($1) ON CONFLICT ("id") DO NOTHING`
	if got[0].SQL != want {
		t.Fatalf("sql:\n got %s\nwant %s", got[0].SQL, want)
	}
}

func TestStatementsWithoutKeyIsPlainInsert(t *testing.T) {
	item := createEvent()
	item.Keys = nil
	got, err := NewBuilder("public").Statements(item)
	if err != nil {
		t.Fatal(err)
	}
	want := `INSERT INTO "public"."users" ("id", "name") VALUES ($1, $2)`
	if got[0].SQL != want {
		t.Fatalf("sql:\n got %s\nwant %s", got[0].SQL, want)
	}
}

func TestStatementsUpdateWithoutKeyChange(t *testing.T) {
	item := createEvent()
	item.Op = event.OpUpdate
	item.Before = map[string]any{"id": int64(1), "name": "old"}
	got, err := NewBuilder("public").Statements(item)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Op != event.OpUpdate {
		t.Fatalf("statements %#v", got)
	}
}

func TestStatementsUpdateWithKeyChangeIsDeleteThenUpsert(t *testing.T) {
	item := createEvent()
	item.Op = event.OpUpdate
	item.Before = map[string]any{"id": int64(1), "name": "alice"}
	item.After = map[string]any{"id": int64(2), "name": "alice"}
	got, err := NewBuilder("public").Statements(item)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("statements %#v", got)
	}
	if got[0].SQL != `DELETE FROM "public"."users" WHERE "id" = $1` {
		t.Fatalf("delete sql %s", got[0].SQL)
	}
	if len(got[0].Args) != 1 || got[0].Args[0] != int64(1) {
		t.Fatalf("delete args %#v", got[0].Args)
	}
	if got[1].Args[0] != int64(2) {
		t.Fatalf("upsert args %#v", got[1].Args)
	}
}

func TestStatementsDelete(t *testing.T) {
	item := createEvent()
	item.Op = event.OpDelete
	item.Before = map[string]any{"id": int64(9)}
	got, err := NewBuilder("public").Statements(item)
	if err != nil {
		t.Fatal(err)
	}
	if got[0].SQL != `DELETE FROM "public"."users" WHERE "id" = $1` {
		t.Fatalf("sql %s", got[0].SQL)
	}
	if got[0].Op != event.OpDelete {
		t.Fatalf("op %s", got[0].Op)
	}
}

func TestStatementsDeleteBeforeImage(t *testing.T) {
	tests := []struct {
		name    string
		keys    []string
		before  map[string]any
		wantSQL string
		wantErr string
	}{
		{
			name:    "nil before image",
			keys:    []string{"id"},
			before:  nil,
			wantErr: `key "id" is not in the before image`,
		},
		{
			name:    "missing single key",
			keys:    []string{"id"},
			before:  map[string]any{"name": "alice"},
			wantErr: `key "id" is not in the before image`,
		},
		{
			name:    "composite key misses one column",
			keys:    []string{"tenant", "id"},
			before:  map[string]any{"tenant": int64(1), "name": "alice"},
			wantErr: `key "id" is not in the before image`,
		},
		{
			name:    "key present but NULL",
			keys:    []string{"id"},
			before:  map[string]any{"id": nil},
			wantErr: `key "id" is NULL in the before image`,
		},
		{
			name:    "valid single key",
			keys:    []string{"id"},
			before:  map[string]any{"id": int64(7)},
			wantSQL: `DELETE FROM "public"."users" WHERE "id" = $1`,
		},
		{
			name:    "valid composite key",
			keys:    []string{"tenant", "id"},
			before:  map[string]any{"tenant": int64(1), "id": int64(7)},
			wantSQL: `DELETE FROM "public"."users" WHERE "tenant" = $1 AND "id" = $2`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			item := createEvent()
			item.Op = event.OpDelete
			item.Keys = tc.keys
			item.Before = tc.before
			got, err := NewBuilder("public").Statements(item)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got %#v", tc.wantErr, got)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0].SQL != tc.wantSQL {
				t.Fatalf("statements %#v", got)
			}
		})
	}
}

func TestStatementsUpdateKeyChangeRequiresBeforeKey(t *testing.T) {
	item := createEvent()
	item.Op = event.OpUpdate
	item.Before = map[string]any{"name": "alice"}
	item.After = map[string]any{"id": int64(2), "name": "alice"}
	if _, err := NewBuilder("public").Statements(item); err == nil {
		t.Fatal("expected error")
	}
}

func TestStatementsDeleteWithoutKeyFails(t *testing.T) {
	item := createEvent()
	item.Op = event.OpDelete
	item.Keys = nil
	item.Before = map[string]any{"id": int64(9)}
	if _, err := NewBuilder("public").Statements(item); err == nil {
		t.Fatal("expected error")
	}
}

func TestStatementsConflictKeyMustBeWritten(t *testing.T) {
	item := createEvent()
	item.Columns = []string{"name"}
	item.After = map[string]any{"id": int64(1), "name": "alice"}
	if _, err := NewBuilder("public").Statements(item); err == nil {
		t.Fatal("expected error")
	}
}

func TestStatementsUpsertKeyImage(t *testing.T) {
	tests := []struct {
		name    string
		keys    []string
		columns []string
		after   map[string]any
		wantSQL string
		wantErr string
	}{
		{
			name:    "nil after image",
			keys:    []string{"id"},
			columns: []string{"id", "name"},
			after:   nil,
			wantErr: `key "id" is not in the after image`,
		},
		{
			name:    "missing single key",
			keys:    []string{"id"},
			columns: []string{"id", "name"},
			after:   map[string]any{"name": "alice"},
			wantErr: `key "id" is not in the after image`,
		},
		{
			name:    "key present but NULL",
			keys:    []string{"id"},
			columns: []string{"id", "name"},
			after:   map[string]any{"id": nil, "name": "alice"},
			wantErr: `key "id" is NULL in the after image`,
		},
		{
			name:    "composite key misses one column",
			keys:    []string{"tenant", "id"},
			columns: []string{"tenant", "id", "name"},
			after:   map[string]any{"tenant": int64(1), "name": "alice"},
			wantErr: `key "id" is not in the after image`,
		},
		{
			name:    "composite key has a NULL column",
			keys:    []string{"tenant", "id"},
			columns: []string{"tenant", "id", "name"},
			after:   map[string]any{"tenant": nil, "id": int64(2), "name": "alice"},
			wantErr: `key "tenant" is NULL in the after image`,
		},
		{
			name:    "valid single key",
			keys:    []string{"id"},
			columns: []string{"id", "name"},
			after:   map[string]any{"id": int64(1), "name": "alice"},
			wantSQL: `INSERT INTO "public"."users" ("id", "name") VALUES ($1, $2) ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name"`,
		},
		{
			name:    "valid composite key",
			keys:    []string{"tenant", "id"},
			columns: []string{"tenant", "id", "name"},
			after:   map[string]any{"tenant": int64(1), "id": int64(2), "name": "alice"},
			wantSQL: `INSERT INTO "public"."users" ("tenant", "id", "name") VALUES ($1, $2, $3) ON CONFLICT ("tenant", "id") DO UPDATE SET "name" = EXCLUDED."name"`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			item := createEvent()
			item.Keys = tc.keys
			item.Columns = tc.columns
			item.After = tc.after
			got, err := NewBuilder("public").Statements(item)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got %#v", tc.wantErr, got)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err, tc.wantErr)
				}
				if got != nil {
					t.Fatalf("a rejected event must not produce statements: %#v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0].SQL != tc.wantSQL {
				t.Fatalf("statements %#v", got)
			}
		})
	}
}

func TestStatementsUpdateWithoutKeyIsPlainInsert(t *testing.T) {
	item := createEvent()
	item.Op = event.OpUpdate
	item.Keys = nil
	item.Before = map[string]any{"id": int64(1), "name": "old"}
	got, err := NewBuilder("public").Statements(item)
	if err != nil {
		t.Fatal(err)
	}
	want := `INSERT INTO "public"."users" ("id", "name") VALUES ($1, $2)`
	if len(got) != 1 || got[0].SQL != want {
		t.Fatalf("statements %#v", got)
	}
}

func TestStatementsKeyChangeWithInvalidAfterKeyFails(t *testing.T) {
	tests := []struct {
		name    string
		after   map[string]any
		wantErr string
	}{
		{
			name:    "missing after key",
			after:   map[string]any{"name": "alice"},
			wantErr: `key "id" is not in the after image`,
		},
		{
			name:    "NULL after key",
			after:   map[string]any{"id": nil, "name": "alice"},
			wantErr: `key "id" is NULL in the after image`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			item := createEvent()
			item.Op = event.OpUpdate
			item.Before = map[string]any{"id": int64(1), "name": "alice"}
			item.After = tc.after
			got, err := NewBuilder("public").Statements(item)
			if err == nil {
				t.Fatalf("expected error containing %q, got %#v", tc.wantErr, got)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
			// The delete of the old key must not be handed out on its own.
			if got != nil {
				t.Fatalf("a rejected event must not produce statements: %#v", got)
			}
		})
	}
}

func TestStatementsWithoutColumnsFails(t *testing.T) {
	item := createEvent()
	item.Columns = nil
	item.After = map[string]any{}
	if _, err := NewBuilder("public").Statements(item); err == nil {
		t.Fatal("expected error")
	}
}

func TestStatementsUnsupportedOpFails(t *testing.T) {
	item := createEvent()
	item.Op = event.OpDDL
	if _, err := NewBuilder("public").Statements(item); err == nil {
		t.Fatal("expected error")
	}
}

func TestStatementsColumnFallbackIsSorted(t *testing.T) {
	item := createEvent()
	item.Columns = nil
	item.Keys = []string{"b"}
	item.After = map[string]any{"b": int64(2), "a": "x"}
	got, err := NewBuilder("public").Statements(item)
	if err != nil {
		t.Fatal(err)
	}
	want := `INSERT INTO "public"."users" ("a", "b") VALUES ($1, $2) ON CONFLICT ("b") DO UPDATE SET "a" = EXCLUDED."a"`
	if got[0].SQL != want {
		t.Fatalf("sql:\n got %s\nwant %s", got[0].SQL, want)
	}
}

func TestStatementsQuoteReservedIdentifiers(t *testing.T) {
	item := createEvent()
	item.Database = "my schema"
	item.Table = `Weird"Name`
	item.SourceDatabase = "app"
	item.Columns = []string{"select", "Mixed Case"}
	item.Keys = []string{"select"}
	item.After = map[string]any{"select": int64(1), "Mixed Case": "x"}
	got, err := NewBuilder("public").Statements(item)
	if err != nil {
		t.Fatal(err)
	}
	want := `INSERT INTO "my schema"."Weird""Name" ("select", "Mixed Case") VALUES ($1, $2) ON CONFLICT ("select") DO UPDATE SET "Mixed Case" = EXCLUDED."Mixed Case"`
	if got[0].SQL != want {
		t.Fatalf("sql:\n got %s\nwant %s", got[0].SQL, want)
	}
}

func TestBuilderCachesTemplate(t *testing.T) {
	builder := NewBuilder("public")
	first, err := builder.Statements(createEvent())
	if err != nil {
		t.Fatal(err)
	}
	second, err := builder.Statements(createEvent())
	if err != nil {
		t.Fatal(err)
	}
	if first[0].SQL != second[0].SQL {
		t.Fatal("cached template differs")
	}
	builder.mu.Lock()
	cached := len(builder.cache)
	builder.mu.Unlock()
	if cached != 1 {
		t.Fatalf("cache entries %d", cached)
	}
}

func TestBackoffDelay(t *testing.T) {
	want := []int{100, 200, 400, 800, 1600, 2000, 2000}
	for i, ms := range want {
		if got := backoffDelay(i + 1); got.Milliseconds() != int64(ms) {
			t.Fatalf("attempt %d: got %s want %dms", i+1, got, ms)
		}
	}
}
