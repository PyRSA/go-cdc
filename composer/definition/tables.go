package definition

import (
	"regexp"
	"strings"
)

var systemSchemas = map[string]struct{}{
	"mysql":              {},
	"information_schema": {},
	"performance_schema": {},
	"sys":                {},
}

// Selector matches database.table names against include and exclude patterns.
// Patterns are regular expressions. The dot in db1\.\.* is a literal dot.
type Selector struct {
	include []*regexp.Regexp
	exclude []*regexp.Regexp
}

// CompileSelector builds a selector from comma-separated regular expressions.
func CompileSelector(include, exclude string) (*Selector, error) {
	inc, err := compileList("source.tables", include)
	if err != nil {
		return nil, err
	}
	if len(inc) == 0 {
		return nil, configErr("source.tables", "required")
	}
	exc, err := compileList("source.tables.exclude", exclude)
	if err != nil {
		return nil, err
	}
	return &Selector{include: inc, exclude: exc}, nil
}

func compileList(field, raw string) ([]*regexp.Regexp, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var out []*regexp.Regexp
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		re, err := regexp.Compile(part)
		if err != nil {
			return nil, configf(field, "invalid pattern %q: %v", part, err)
		}
		out = append(out, re)
	}
	return out, nil
}

// Match reports whether a user table is captured.
// mysql, information_schema, performance_schema, and sys never match.
func (s *Selector) Match(database, table string) bool {
	if s == nil {
		return false
	}
	if _, blocked := systemSchemas[strings.ToLower(database)]; blocked {
		return false
	}
	id := database + "." + table
	included := false
	for _, re := range s.include {
		if fullMatch(re, id) {
			included = true
			break
		}
	}
	if !included {
		return false
	}
	for _, re := range s.exclude {
		if fullMatch(re, id) {
			return false
		}
	}
	return true
}

// fullMatch reports whether re matches the whole identifier.
// A pattern for one table must not also capture a longer name.
func fullMatch(re *regexp.Regexp, value string) bool {
	loc := re.FindStringIndex(value)
	return loc != nil && loc[0] == 0 && loc[1] == len(value)
}
