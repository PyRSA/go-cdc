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
