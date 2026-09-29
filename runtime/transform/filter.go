package transform

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"
)

type expr interface {
	eval(row map[string]any) bool
}

type binary struct {
	op          string
	left, right expr
}

func (b binary) eval(row map[string]any) bool {
	switch b.op {
	case "AND":
		return b.left.eval(row) && b.right.eval(row)
	case "OR":
		return b.left.eval(row) || b.right.eval(row)
	default:
		return false
	}
}

type not struct{ inner expr }

func (n not) eval(row map[string]any) bool { return !n.inner.eval(row) }

type predicate struct {
	column string
	op     string
	value  any
	list   []any
}

func (p predicate) eval(row map[string]any) bool {
	value, ok := lookup(row, p.column)
	if p.op != "IS NULL" && p.op != "IS NOT NULL" && !ok && p.op != "IN" {
		return false
	}
	switch p.op {
	case "IS NULL":
		return !ok || value == nil
	case "IS NOT NULL":
		return ok && value != nil
	case "IN":
		if !ok || value == nil {
			return false
		}
		for _, item := range p.list {
			if equal(value, item) {
				return true
			}
		}
		return false
	case "LIKE":
		if value == nil {
			return false
		}
		pattern, _ := p.value.(string)
		return like(fmt.Sprint(value), pattern)
	default:
		if value == nil || p.value == nil {
			return false
		}
		return compare(value, p.value, p.op)
	}
}

func lookup(row map[string]any, column string) (any, bool) {
	if row == nil {
		return nil, false
	}
	if value, ok := row[column]; ok {
		return value, true
	}
	for key, value := range row {
		if strings.EqualFold(key, column) {
			return value, true
		}
	}
	return nil, false
}

func equal(left, right any) bool { return compare(left, right, "=") }

func compare(left, right any, op string) bool {
	if li, lok := asRat(left); lok {
		if ri, rok := asRat(right); rok {
			cmp := li.Cmp(ri)
			return cmpResult(cmp, op)
		}
	}
	cmp := strings.Compare(fmt.Sprint(left), fmt.Sprint(right))
	return cmpResult(cmp, op)
}

func cmpResult(cmp int, op string) bool {
	switch op {
	case "=", "==":
		return cmp == 0
	case "!=", "<>":
		return cmp != 0
	case "<":
		return cmp < 0
	case ">":
		return cmp > 0
	case "<=":
		return cmp <= 0
	case ">=":
		return cmp >= 0
	default:
		return false
	}
}

func asRat(value any) (*big.Rat, bool) {
	switch typed := value.(type) {
	case int:
		return new(big.Rat).SetInt64(int64(typed)), true
	case int32:
		return new(big.Rat).SetInt64(int64(typed)), true
	case int64:
		return new(big.Rat).SetInt64(typed), true
	case uint32:
		return new(big.Rat).SetUint64(uint64(typed)), true
	case uint64:
		rat := new(big.Rat)
		rat.SetInt(new(big.Int).SetUint64(typed))
		return rat, true
	case string:
		rat := new(big.Rat)
		if _, ok := rat.SetString(typed); ok {
			return rat, true
		}
		return nil, false
	case []byte:
		rat := new(big.Rat)
		if _, ok := rat.SetString(string(typed)); ok {
			return rat, true
		}
		return nil, false
	default:
		return nil, false
	}
}

func like(value, pattern string) bool {
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '\\':
			if i+1 < len(pattern) {
				i++
				b.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
			}
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(pattern[i : i+1]))
		}
	}
	b.WriteString("$")
	re, err := regexp.Compile(b.String())
	if err != nil {
		return false
	}
	return re.MatchString(value)
}

type parser struct {
	s string
	i int
}

func parseFilter(input string) (expr, error) {
	p := &parser{s: input}
	expr, err := p.parseOr()
	if err != nil {
		return nil, err
	}
	p.skip()
	if p.i != len(p.s) {
		return nil, fmt.Errorf("unexpected %q", p.s[p.i:])
	}
	return expr, nil
}

func (p *parser) parseOr() (expr, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for {
		if !p.matchKeyword("OR") {
			return left, nil
		}
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = binary{op: "OR", left: left, right: right}
	}
}

func (p *parser) parseAnd() (expr, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for {
		if !p.matchKeyword("AND") {
			return left, nil
		}
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = binary{op: "AND", left: left, right: right}
	}
}

func (p *parser) parseNot() (expr, error) {
	if p.matchKeyword("NOT") {
		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return not{inner}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (expr, error) {
	p.skip()
	if p.peek() == '(' {
		p.i++
		inner, err := p.parseOr()
		if err != nil {
			return nil, err
		}
		p.skip()
		if p.peek() != ')' {
			return nil, fmt.Errorf("missing )")
		}
		p.i++
		return inner, nil
	}
	column, err := p.ident()
	if err != nil {
		return nil, err
	}
	p.skip()
	if p.matchKeyword("IS") {
		notNull := p.matchKeyword("NOT")
		if !p.matchKeyword("NULL") {
			return nil, fmt.Errorf("expected NULL")
		}
		op := "IS NULL"
		if notNull {
			op = "IS NOT NULL"
		}
		return predicate{column: column, op: op}, nil
	}
	if p.matchKeyword("IN") {
		p.skip()
		if p.peek() != '(' {
			return nil, fmt.Errorf("expected (")
		}
		p.i++
		var list []any
		for {
			p.skip()
			value, err := p.literal()
			if err != nil {
				return nil, err
			}
			list = append(list, value)
			p.skip()
			if p.peek() == ',' {
				p.i++
				continue
			}
			if p.peek() == ')' {
				p.i++
				break
			}
			return nil, fmt.Errorf("expected , or )")
		}
		return predicate{column: column, op: "IN", list: list}, nil
	}
	if p.matchKeyword("LIKE") {
		value, err := p.literal()
		if err != nil {
			return nil, err
		}
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("LIKE pattern must be a string")
		}
		return predicate{column: column, op: "LIKE", value: text}, nil
	}
	op, err := p.operator()
	if err != nil {
		return nil, err
	}
	value, err := p.literal()
	if err != nil {
		return nil, err
	}
	return predicate{column: column, op: op, value: value}, nil
}

func (p *parser) operator() (string, error) {
	p.skip()
	ops := []string{"<>", "!=", "<=", ">=", "=", "<", ">"}
	for _, op := range ops {
		if strings.HasPrefix(p.s[p.i:], op) {
			p.i += len(op)
			return op, nil
		}
	}
	return "", fmt.Errorf("expected comparison")
}

func (p *parser) literal() (any, error) {
	p.skip()
	if p.matchKeyword("NULL") {
		return nil, nil
	}
	if p.peek() == '\'' {
		return p.string()
	}
	start := p.i
	if p.peek() == '-' || p.peek() == '+' {
		p.i++
	}
	sawDigit := false
	for p.i < len(p.s) && (unicode.IsDigit(rune(p.s[p.i])) || p.s[p.i] == '.') {
		if p.s[p.i] != '.' {
			sawDigit = true
		}
		p.i++
	}
	if !sawDigit {
		return nil, fmt.Errorf("expected literal")
	}
	return p.s[start:p.i], nil
}

func (p *parser) string() (string, error) {
	p.i++
	var b strings.Builder
	for p.i < len(p.s) {
		if p.s[p.i] == '\'' {
			if p.i+1 < len(p.s) && p.s[p.i+1] == '\'' {
				b.WriteByte('\'')
				p.i += 2
				continue
			}
			p.i++
			return b.String(), nil
		}
		b.WriteByte(p.s[p.i])
		p.i++
	}
	return "", fmt.Errorf("unterminated string")
}

func (p *parser) ident() (string, error) {
	p.skip()
	if p.peek() == '`' {
		p.i++
		start := p.i
		for p.i < len(p.s) && p.s[p.i] != '`' {
			p.i++
		}
		if p.i >= len(p.s) {
			return "", fmt.Errorf("unterminated identifier")
		}
		name := p.s[start:p.i]
		p.i++
		return name, nil
	}
	start := p.i
	for p.i < len(p.s) {
		r, size := utf8.DecodeRuneInString(p.s[p.i:])
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '$' {
			break
		}
		if start == p.i && unicode.IsDigit(r) {
			break
		}
		p.i += size
	}
	if p.i == start {
		return "", fmt.Errorf("expected column")
	}
	return p.s[start:p.i], nil
}

func (p *parser) matchKeyword(word string) bool {
	p.skip()
	if p.i+len(word) > len(p.s) {
		return false
	}
	if !strings.EqualFold(p.s[p.i:p.i+len(word)], word) {
		return false
	}
	end := p.i + len(word)
	if end < len(p.s) {
		r, _ := utf8.DecodeRuneInString(p.s[end:])
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			return false
		}
	}
	p.i = end
	return true
}

func (p *parser) skip() {
	for p.i < len(p.s) && unicode.IsSpace(rune(p.s[p.i])) {
		p.i++
	}
}

func (p *parser) peek() byte {
	if p.i >= len(p.s) {
		return 0
	}
	return p.s[p.i]
}
