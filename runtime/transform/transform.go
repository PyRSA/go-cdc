// Package transform applies projection, filter, and business keys.
// The first matching rule wins. Computed columns and functions are out of scope.
package transform

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
)

// Rule is one compiled transform entry.
type Rule struct {
	source     *regexp.Regexp
	columns    []string
	allColumns bool
	filter     expr
	keys       []string
}

// Set is the ordered rule list from a pipeline.
type Set struct {
	rules []Rule
}

// Compile prepares transform rules. An unknown column is rejected when the
// source column list is supplied to Validate.
func Compile(rules []definition.Transform) (*Set, error) {
	set := &Set{}
	for i, rule := range rules {
		re, err := regexp.Compile(rule.SourceTable)
		if err != nil {
			return nil, fmt.Errorf("transform item %d: source-table: %w", i, err)
		}
		compiled := Rule{source: re, keys: splitList(rule.PrimaryKeys)}
		projection := splitList(rule.Projection)
		for _, column := range projection {
			if column == "*" {
				if len(projection) != 1 {
					return nil, fmt.Errorf("transform item %d: projection * cannot be combined with other columns", i)
				}
				compiled.allColumns = true
			}
		}
		if !compiled.allColumns {
			compiled.columns = projection
		}
		if strings.TrimSpace(rule.Filter) != "" {
			filter, err := parseFilter(rule.Filter)
			if err != nil {
				return nil, fmt.Errorf("transform item %d: filter: %w", i, err)
			}
			compiled.filter = filter
		}
		set.rules = append(set.rules, compiled)
	}
	return set, nil
}

// Validate checks a projection against the columns of one source table.
func (s *Set) Validate(database, table string, columns []string) error {
	if s == nil {
		return nil
	}
	rule, ok := s.match(database, table)
	if !ok || rule.allColumns || len(rule.columns) == 0 {
		return nil
	}
	known := map[string]struct{}{}
	for _, column := range columns {
		known[strings.ToLower(column)] = struct{}{}
	}
	for _, column := range rule.columns {
		if _, found := known[strings.ToLower(column)]; !found {
			return fmt.Errorf("transform.projection: column %s is not on %s.%s", column, database, table)
		}
	}
	return nil
}

// ProjectTable keeps the projected columns and the sink business key.
// A primary key that the projection drops is not copied onto the sink table.
func (s *Set) ProjectTable(table schema.Table) schema.Table {
	out := schema.Table{
		Database:    table.Database,
		Table:       table.Table,
		Columns:     append([]schema.Column(nil), table.Columns...),
		PrimaryKeys: append([]string(nil), table.PrimaryKeys...),
	}
	if s == nil {
		return out
	}
	rule, ok := s.match(table.Database, table.Table)
	if !ok {
		return out
	}
	if len(rule.columns) > 0 {
		byName := map[string]schema.Column{}
		for _, column := range table.Columns {
			byName[strings.ToLower(column.Name)] = column
		}
		cols := make([]schema.Column, 0, len(rule.columns))
		for _, name := range rule.columns {
			column, found := byName[strings.ToLower(name)]
			if found {
				cols = append(cols, column)
			}
		}
		out.Columns = cols
	}
	if len(rule.keys) > 0 {
		out.PrimaryKeys = append([]string(nil), rule.keys...)
		return out
	}
	keep := map[string]struct{}{}
	for _, column := range out.Columns {
		keep[strings.ToLower(column.Name)] = struct{}{}
	}
	keys := make([]string, 0, len(out.PrimaryKeys))
	for _, key := range out.PrimaryKeys {
		if _, found := keep[strings.ToLower(key)]; found {
			keys = append(keys, key)
		}
	}
	out.PrimaryKeys = keys
	return out
}

// Apply projects and filters one event.
// Filter is evaluated on before/after separately so the sink stays aligned with
// "source ∩ filter":
//   - create: keep only when after matches
//   - delete: keep only when before matches
//   - update stay-in: keep as update
//   - update fall-out (before match, after miss): emit delete of before
//   - update fall-in (before miss, after match): emit create of after
//   - update stay-out: drop
//
// DDL events are returned unchanged. A miss keeps every column.
func (s *Set) Apply(in event.Event) (event.Event, bool) {
	if in.Op == event.OpDDL || s == nil {
		return in, true
	}
	rule, ok := s.match(in.SourceDatabase, in.SourceTable)
	if !ok {
		if in.SourceDatabase == "" {
			rule, ok = s.match(in.Database, in.Table)
		}
	}
	if !ok {
		return in, true
	}
	if rule.filter != nil {
		beforeOK := in.Before != nil && rule.filter.eval(in.Before)
		afterOK := in.After != nil && rule.filter.eval(in.After)
		switch in.Op {
		case event.OpCreate, event.OpRead:
			if !afterOK {
				return event.Event{}, false
			}
		case event.OpDelete:
			if !beforeOK {
				return event.Event{}, false
			}
		case event.OpUpdate:
			switch {
			case beforeOK && afterOK:
				// stay in
			case beforeOK && !afterOK:
				in.Op = event.OpDelete
				in.After = nil
			case !beforeOK && afterOK:
				in.Op = event.OpCreate
				in.Before = nil
			default:
				return event.Event{}, false
			}
		default:
			if !rule.filter.eval(image(in)) {
				return event.Event{}, false
			}
		}
	}
	if len(rule.columns) > 0 {
		in.Before = project(in.Before, rule.columns)
		in.After = project(in.After, rule.columns)
		in.Columns = append([]string(nil), rule.columns...)
	}
	if len(rule.keys) > 0 {
		in.Keys = append([]string(nil), rule.keys...)
	}
	return in, true
}

// ProjectedColumns returns the explicit projection for a source table.
// A nil result means every column is kept.
func (s *Set) ProjectedColumns(database, table string) []string {
	if s == nil {
		return nil
	}
	rule, ok := s.match(database, table)
	if !ok || rule.allColumns || len(rule.columns) == 0 {
		return nil
	}
	return append([]string(nil), rule.columns...)
}

func (s *Set) match(database, table string) (Rule, bool) {
	id := database + "." + table
	for _, rule := range s.rules {
		if fullMatch(rule.source, id) {
			return rule, true
		}
	}
	return Rule{}, false
}

func fullMatch(re *regexp.Regexp, value string) bool {
	loc := re.FindStringIndex(value)
	return loc != nil && loc[0] == 0 && loc[1] == len(value)
}

func image(in event.Event) map[string]any {
	if in.Op == event.OpDelete {
		return in.Before
	}
	if in.After != nil {
		return in.After
	}
	return in.Before
}

func project(row map[string]any, columns []string) map[string]any {
	if row == nil {
		return nil
	}
	out := make(map[string]any, len(columns))
	for _, column := range columns {
		value, ok := lookup(row, column)
		if ok {
			out[column] = value
		} else {
			out[column] = nil
		}
	}
	return out
}

func splitList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		part = strings.Trim(part, "`")
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
