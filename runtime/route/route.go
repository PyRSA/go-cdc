// Package route rewrites source tables onto sink tables.
package route

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/composer/definition"
)

// Rule is one compiled route.
type Rule struct {
	source *regexp.Regexp
	sinkDB string
	sink   string
	symbol string
}

// Set applies ALL_MATCH or FIRST_MATCH.
type Set struct {
	rules []Rule
	mode  string
}

// Compile prepares route rules.
func Compile(rules []definition.Route, mode string) (*Set, error) {
	set := &Set{mode: mode}
	if set.mode == "" {
		set.mode = definition.RouteAllMatch
	}
	for i, rule := range rules {
		re, err := regexp.Compile(rule.SourceTable)
		if err != nil {
			return nil, fmt.Errorf("route item %d: source-table: %w", i, err)
		}
		db, table, ok := splitQualified(rule.SinkTable)
		if !ok {
			return nil, fmt.Errorf("route item %d: sink-table must be database.table", i)
		}
		set.rules = append(set.rules, Rule{source: re, sinkDB: db, sink: table, symbol: rule.ReplaceSymbol})
	}
	return set, nil
}

// Apply returns one event per matching sink table.
// No match keeps the source names.
func (s *Set) Apply(in event.Event) []event.Event {
	if s == nil || len(s.rules) == 0 {
		return []event.Event{in}
	}
	db, table := in.SourceDatabase, in.SourceTable
	if db == "" {
		db, table = in.Database, in.Table
	}
	id := db + "." + table
	var out []event.Event
	for _, rule := range s.rules {
		if !fullMatch(rule.source, id) {
			continue
		}
		cloned := in.Clone()
		sinkTable := rule.sink
		if rule.symbol != "" {
			sinkTable = strings.ReplaceAll(sinkTable, rule.symbol, table)
		}
		cloned.Database = rule.sinkDB
		cloned.Table = sinkTable
		out = append(out, cloned)
		if s.mode == definition.RouteFirstMatch {
			break
		}
	}
	if len(out) == 0 {
		return []event.Event{in}
	}
	return out
}

func fullMatch(re *regexp.Regexp, value string) bool {
	loc := re.FindStringIndex(value)
	return loc != nil && loc[0] == 0 && loc[1] == len(value)
}

func splitQualified(name string) (string, string, bool) {
	i := strings.IndexByte(name, '.')
	if i <= 0 || i == len(name)-1 {
		return "", "", false
	}
	return name[:i], name[i+1:], true
}
