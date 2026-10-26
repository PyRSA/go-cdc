package composer

import (
	"fmt"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/connector/sink"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
	"github.com/PyRSA/go-cdc/runtime/route"
	"github.com/PyRSA/go-cdc/runtime/transform"
)

// CatalogSink is an optional sink capability. The runner hands it every final
// routed table before the first event, so a sink can prepare external
// structures (Elasticsearch indexes) and fail startup on incompatible
// mappings. Sinks that do not implement it keep their existing lifecycle.
//
// PrepareCatalog must be safe to call again for an already prepared table and
// must return the first error instead of skipping it.
type CatalogSink interface {
	PrepareCatalog(tables []schema.Table) error
}

// PrepareCatalog projects and routes the live source tables, then lets a
// CatalogSink prepare them. tables comes from the source catalog; idle sinks
// return immediately.
func PrepareCatalog(pipe *definition.Pipeline, sk sink.Sink, tables []schema.Table, state checkpoint.State) error {
	catalog, ok := sk.(CatalogSink)
	if !ok {
		return nil
	}
	targets, err := finalTables(pipe, tables, state)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		return nil
	}
	return catalog.PrepareCatalog(targets)
}

// finalTables is the catalog view a CatalogSink must be able to write: the
// transform projection of every captured source table on its routed name.
// Captured tables stay included even when the selector changed, because the
// source may still replay their binlog; a dropped table cannot produce events.
func finalTables(pipe *definition.Pipeline, tables []schema.Table, state checkpoint.State) ([]schema.Table, error) {
	rules, err := transform.Compile(pipe.Transforms)
	if err != nil {
		return nil, err
	}
	routes, err := route.Compile(pipe.Routes, pipe.Runtime.RouteMode)
	if err != nil {
		return nil, err
	}
	var out []schema.Table
	for _, table := range tables {
		if pipe.Source.Tables != nil && !pipe.Source.Tables.Match(table.Database, table.Table) &&
			!state.HasCaptured(table.Database+"."+table.Table) {
			continue
		}
		names := make([]string, len(table.Columns))
		for i, column := range table.Columns {
			names[i] = column.Name
		}
		if err := rules.Validate(table.Database, table.Table, names); err != nil {
			return nil, err
		}
		projected := rules.ProjectTable(table)
		routed := routes.Apply(event.Event{
			Database: table.Database, Table: table.Table,
			SourceDatabase: table.Database, SourceTable: table.Table,
		})
		for _, dst := range routed {
			sinkTable := projected
			sinkTable.Database = dst.Database
			sinkTable.Table = dst.Table
			out = append(out, sinkTable)
		}
	}
	return out, nil
}

// catalogTargets is the routed form of one source table, shared with the
// per-message catalog path.
func catalogTargets(rules *transform.Set, routes *route.Set, table schema.Table) []schema.Table {
	projected := rules.ProjectTable(table)
	routed := routes.Apply(event.Event{
		Database: table.Database, Table: table.Table,
		SourceDatabase: table.Database, SourceTable: table.Table,
	})
	targets := make([]schema.Table, 0, len(routed))
	for _, dst := range routed {
		sinkTable := projected
		sinkTable.Database = dst.Database
		sinkTable.Table = dst.Table
		targets = append(targets, sinkTable)
	}
	return targets
}

// checkCatalogTargets guards the per-message path against an empty target list,
// which would silently skip sink preparation.
func checkCatalogTargets(targets []schema.Table) error {
	if len(targets) == 0 {
		return fmt.Errorf("catalog: no routed sink table")
	}
	return nil
}
