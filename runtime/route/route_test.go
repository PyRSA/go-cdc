package route

import (
	"testing"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/composer/definition"
)

func TestReplaceSymbolAndFixedTable(t *testing.T) {
	set, err := Compile([]definition.Route{
		{SourceTable: `source_db\..*`, SinkTable: "sink_db.<>", ReplaceSymbol: "<>"},
		{SourceTable: `bdp_sifa\.risk_lawsuit`, SinkTable: "judicial.ods_judicial_lawsuit_infos_rl"},
	}, definition.RouteAllMatch)
	if err != nil {
		t.Fatal(err)
	}
	orders := set.Apply(event.Event{SourceDatabase: "source_db", SourceTable: "orders", Database: "source_db", Table: "orders"})
	if len(orders) != 1 || orders[0].Database != "sink_db" || orders[0].Table != "orders" {
		t.Fatalf("%#v", orders)
	}
	if orders[0].SourceTable != "orders" {
		t.Fatal("source table was overwritten")
	}
	lawsuit := set.Apply(event.Event{SourceDatabase: "bdp_sifa", SourceTable: "risk_lawsuit"})
	if lawsuit[0].SinkID() != "judicial.ods_judicial_lawsuit_infos_rl" {
		t.Fatalf("%#v", lawsuit)
	}
}

func TestFirstMatch(t *testing.T) {
	set, err := Compile([]definition.Route{
		{SourceTable: `db\.t`, SinkTable: "a.t1"},
		{SourceTable: `db\.t`, SinkTable: "b.t2"},
	}, definition.RouteFirstMatch)
	if err != nil {
		t.Fatal(err)
	}
	got := set.Apply(event.Event{SourceDatabase: "db", SourceTable: "t"})
	if len(got) != 1 || got[0].Database != "a" {
		t.Fatalf("%#v", got)
	}
}

func TestApplyMarksRouted(t *testing.T) {
	set, err := Compile([]definition.Route{
		{SourceTable: `db\.t`, SinkTable: "db.t"},
		{SourceTable: `db\.renamed`, SinkTable: "db.renamed_v2"},
		{SourceTable: `db\.moved`, SinkTable: "other.moved"},
		{SourceTable: `db\.fan`, SinkTable: "a.fan"},
		{SourceTable: `db\.fan`, SinkTable: "b.fan"},
	}, definition.RouteAllMatch)
	if err != nil {
		t.Fatal(err)
	}

	identity := set.Apply(event.Event{SourceDatabase: "db", SourceTable: "t", Database: "db", Table: "t"})
	if len(identity) != 1 || !identity[0].Routed {
		t.Fatalf("identity route must be marked: %#v", identity)
	}
	if identity[0].SourceDatabase != "db" {
		t.Fatal("source database was overwritten")
	}

	tableOnly := set.Apply(event.Event{SourceDatabase: "db", SourceTable: "renamed", Database: "db", Table: "renamed"})
	if len(tableOnly) != 1 || !tableOnly[0].Routed {
		t.Fatalf("table-only route must be marked: %#v", tableOnly)
	}
	if tableOnly[0].Database != "db" || tableOnly[0].Table != "renamed_v2" {
		t.Fatalf("table-only route rewrote the database: %#v", tableOnly[0])
	}

	moved := set.Apply(event.Event{SourceDatabase: "db", SourceTable: "moved", Database: "db", Table: "moved"})
	if len(moved) != 1 || !moved[0].Routed || moved[0].Database != "other" {
		t.Fatalf("database rewrite not applied: %#v", moved)
	}

	fan := set.Apply(event.Event{SourceDatabase: "db", SourceTable: "fan", Database: "db", Table: "fan", After: map[string]any{"id": 1}})
	if len(fan) != 2 || !fan[0].Routed || !fan[1].Routed {
		t.Fatalf("every fan-out event must be marked: %#v", fan)
	}
}

func TestApplyKeepsUnroutedUnmarked(t *testing.T) {
	set, err := Compile([]definition.Route{
		{SourceTable: `db\.t`, SinkTable: "sink.t"},
	}, definition.RouteAllMatch)
	if err != nil {
		t.Fatal(err)
	}
	missed := set.Apply(event.Event{SourceDatabase: "db", SourceTable: "other", Database: "db", Table: "other"})
	if len(missed) != 1 || missed[0].Routed {
		t.Fatalf("unmatched event must stay unmarked: %#v", missed)
	}
	// A reused event that still carries a mark must not look routed.
	stale := set.Apply(event.Event{SourceDatabase: "db", SourceTable: "other", Database: "sink", Table: "other", Routed: true})
	if stale[0].Routed {
		t.Fatalf("stale mark survived a miss: %#v", stale[0])
	}

	empty, err := Compile(nil, definition.RouteAllMatch)
	if err != nil {
		t.Fatal(err)
	}
	plain := empty.Apply(event.Event{SourceDatabase: "db", SourceTable: "t", Database: "db", Table: "t", Routed: true})
	if plain[0].Routed {
		t.Fatalf("stale mark survived a set without rules: %#v", plain[0])
	}
}

func TestFirstMatchMarksRouted(t *testing.T) {
	set, err := Compile([]definition.Route{
		{SourceTable: `db\.t`, SinkTable: "a.t1"},
		{SourceTable: `db\.t`, SinkTable: "b.t2"},
	}, definition.RouteFirstMatch)
	if err != nil {
		t.Fatal(err)
	}
	got := set.Apply(event.Event{SourceDatabase: "db", SourceTable: "t"})
	if len(got) != 1 || !got[0].Routed || got[0].Database != "a" {
		t.Fatalf("%#v", got)
	}
}

func TestCloneKeepsRouted(t *testing.T) {
	set, err := Compile([]definition.Route{
		{SourceTable: `db\.t`, SinkTable: "a.t"},
		{SourceTable: `db\.t`, SinkTable: "b.t"},
	}, definition.RouteAllMatch)
	if err != nil {
		t.Fatal(err)
	}
	got := set.Apply(event.Event{SourceDatabase: "db", SourceTable: "t", After: map[string]any{"id": 1}})
	for i := range got {
		if !got[i].Routed {
			t.Fatalf("clone %d lost the mark: %#v", i, got[i])
		}
	}
}

func TestAllMatchFanout(t *testing.T) {
	set, err := Compile([]definition.Route{
		{SourceTable: `db\.t`, SinkTable: "a.t1"},
		{SourceTable: `db\.t`, SinkTable: "b.t2"},
	}, definition.RouteAllMatch)
	if err != nil {
		t.Fatal(err)
	}
	got := set.Apply(event.Event{SourceDatabase: "db", SourceTable: "t", After: map[string]any{"id": 1}})
	if len(got) != 2 {
		t.Fatalf("%#v", got)
	}
	got[0].After["id"] = 2
	if got[1].After["id"] != 1 {
		t.Fatal("fan-out shares the row map")
	}
}
