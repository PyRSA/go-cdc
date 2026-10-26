package elasticsearch

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/connector/sink/elasticsearch/client"
)

// tableMeta is the cached, per-target-table write plan.
type tableMeta struct {
	table   schema.Table
	index   string
	scope   string // routed database.table, part of the _id hash
	columns []columnMeta
	byName  map[string]columnMeta
	keys    []columnMeta
}

type columnMeta struct {
	name string
	ct   columnType
	kind valueKind
}

func newTableMeta(table schema.Table, index string) (*tableMeta, error) {
	meta := &tableMeta{
		table:  table,
		index:  index,
		scope:  table.Database + "." + table.Table,
		byName: make(map[string]columnMeta, len(table.Columns)),
	}
	for _, column := range table.Columns {
		ct, err := parseColumnType(column.Type)
		if err != nil {
			return nil, fmt.Errorf("table %s.%s: column %q: %v", table.Database, table.Table, column.Name, err)
		}
		cm := columnMeta{name: column.Name, ct: ct, kind: kindOf(ct)}
		meta.columns = append(meta.columns, cm)
		meta.byName[column.Name] = cm
	}
	for _, key := range table.PrimaryKeys {
		cm, ok := meta.byName[key]
		if !ok {
			return nil, fmt.Errorf("table %s.%s: primary key %q is not one of the projected columns",
				table.Database, table.Table, key)
		}
		meta.keys = append(meta.keys, cm)
	}
	return meta, nil
}

// samePlan reports whether the cached plan already serves this exact routed
// schema. Column order, column types, and the primary key order all feed the
// write plan, so any difference forces a full re-check.
func (m *tableMeta) samePlan(table schema.Table) bool {
	if m.scope != table.Database+"."+table.Table {
		return false
	}
	if len(m.table.Columns) != len(table.Columns) || len(m.table.PrimaryKeys) != len(table.PrimaryKeys) {
		return false
	}
	for i, key := range m.table.PrimaryKeys {
		if key != table.PrimaryKeys[i] {
			return false
		}
	}
	for i, column := range m.table.Columns {
		other := table.Columns[i]
		if column.Name != other.Name || !strings.EqualFold(column.Type, other.Type) {
			return false
		}
	}
	return true
}

// operationFor turns one event into a bulk operation with a stable _id. Every
// failure names the operation, the capturing table, and the routed table.
func (s *Sink) operationFor(ev event.Event) (client.BulkOperation, error) {
	meta, ok := s.metaFor(ev.SinkID())
	if !ok {
		return client.BulkOperation{}, fmt.Errorf(
			"elasticsearch: %s source %s -> %s: table was not registered by the catalog before the write",
			ev.Op, ev.SourceID(), ev.SinkID())
	}
	op, err := meta.operation(ev)
	if err != nil {
		return client.BulkOperation{}, fmt.Errorf(
			"elasticsearch: %s source %s -> %s: %w", ev.Op, ev.SourceID(), ev.SinkID(), err)
	}
	return op, nil
}

func (m *tableMeta) operation(ev event.Event) (client.BulkOperation, error) {
	switch ev.Op {
	case event.OpRead, event.OpCreate, event.OpUpdate:
		if ev.After == nil {
			return client.BulkOperation{}, errors.New("event has no After document")
		}
		if err := m.checkEventKeys(ev); err != nil {
			return client.BulkOperation{}, err
		}
		id, err := m.idFor(ev.After)
		if err != nil {
			return client.BulkOperation{}, err
		}
		doc, err := m.document(ev.After)
		if err != nil {
			return client.BulkOperation{}, err
		}
		return client.BulkOperation{Action: client.ActionIndex, Index: m.index, ID: id, Document: doc}, nil
	case event.OpDelete:
		if ev.Before == nil {
			return client.BulkOperation{}, errors.New("delete has no Before row")
		}
		if err := m.checkEventKeys(ev); err != nil {
			return client.BulkOperation{}, err
		}
		id, err := m.idFor(ev.Before)
		if err != nil {
			return client.BulkOperation{}, err
		}
		return client.BulkOperation{Action: client.ActionDelete, Index: m.index, ID: id}, nil
	default:
		return client.BulkOperation{}, fmt.Errorf("operation %q is not supported by the sink", ev.Op)
	}
}

// checkEventKeys rejects events whose key metadata disagrees with the final
// Schema. The Schema order stays authoritative for _id encoding.
func (m *tableMeta) checkEventKeys(ev event.Event) error {
	if len(ev.Keys) == 0 {
		return nil
	}
	if len(ev.Keys) != len(m.keys) {
		return fmt.Errorf(
			"event keys %v disagree with the table primary key %v", ev.Keys, m.table.PrimaryKeys)
	}
	for _, key := range ev.Keys {
		found := false
		for _, expected := range m.table.PrimaryKeys {
			if strings.EqualFold(key, expected) {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf(
				"event key %q is not part of the table primary key %v", key, m.table.PrimaryKeys)
		}
	}
	return nil
}

// idFor hashes the routed table identity plus the ordered primary key values.
// Length prefixes make the encoding unambiguous for any value content.
func (m *tableMeta) idFor(row map[string]any) (string, error) {
	if len(m.keys) == 0 {
		return "", fmt.Errorf("elasticsearch: table %s has no primary key; v1 cannot build a stable _id", m.scope)
	}
	digest := sha256.New()
	writePart(digest, m.scope)
	for _, key := range m.keys {
		value, ok := lookupCase(row, key.name)
		if !ok {
			return "", fmt.Errorf("elasticsearch: table %s: primary key %q is missing from the event", m.scope, key.name)
		}
		if value == nil {
			return "", fmt.Errorf("elasticsearch: table %s: primary key %q is NULL", m.scope, key.name)
		}
		text, err := canonicalValue(key, value)
		if err != nil {
			return "", fmt.Errorf("elasticsearch: table %s: primary key %q: %w", m.scope, key.name, err)
		}
		writePart(digest, key.name)
		writePart(digest, text)
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

func writePart(digest hash.Hash, value string) {
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(value)))
	_, _ = digest.Write(size[:])
	_, _ = digest.Write([]byte(value))
}

// document builds the JSON document for one row using the canonical value form.
func (m *tableMeta) document(row map[string]any) ([]byte, error) {
	doc := make(map[string]json.RawMessage, len(m.columns))
	for _, column := range m.columns {
		value, ok := lookupCase(row, column.name)
		if !ok {
			return nil, fmt.Errorf(
				"elasticsearch: table %s: column %q is missing from the event; check projection and primary-keys",
				m.scope, column.name)
		}
		encoded, err := jsonValue(column, value)
		if err != nil {
			return nil, fmt.Errorf("elasticsearch: table %s: column %q: %w", m.scope, column.name, err)
		}
		doc[column.name] = encoded
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch: table %s: encode document: %w", m.scope, err)
	}
	return body, nil
}

func jsonValue(column columnMeta, value any) (json.RawMessage, error) {
	if value == nil {
		return json.RawMessage("null"), nil
	}
	if column.kind == kindJSON {
		raw, err := jsonText(value)
		if err != nil {
			return nil, err
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, []byte(raw)); err != nil {
			return nil, fmt.Errorf("value is not valid JSON: %v", err)
		}
		return json.RawMessage(compact.String()), nil
	}
	text, err := canonicalValue(column, value)
	if err != nil {
		return nil, err
	}
	switch column.kind {
	case kindInteger, kindFloat:
		return json.RawMessage(text), nil
	default:
		return json.Marshal(text)
	}
}

// canonicalValue normalizes a source value for its column. Both the document
// and the _id hash use this form, so snapshot and binlog agree.
func canonicalValue(column columnMeta, value any) (string, error) {
	switch column.kind {
	case kindInteger:
		if column.ct.Unsigned {
			n, err := asUint64(value)
			if err != nil {
				return "", err
			}
			return strconv.FormatUint(n, 10), nil
		}
		n, err := asInt64(value)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(n, 10), nil
	case kindFloat:
		bits := 64
		if column.ct.Base == "float" {
			bits = 32
		}
		f, err := asFloat64(value, bits)
		if err != nil {
			return "", err
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return "", fmt.Errorf("value %v cannot be represented in JSON", f)
		}
		return strconv.FormatFloat(f, 'g', -1, bits), nil
	case kindDecimal:
		text, err := asText(value)
		if err != nil {
			return "", err
		}
		return canonicalDecimal(text, column.ct.Scale)
	case kindString:
		return asText(value)
	case kindEnumSet:
		return canonicalEnumSet(column.ct, value)
	case kindDate:
		if column.ct.Base == "year" {
			return canonicalYear(column.ct, value)
		}
		text, err := asText(value)
		if err != nil {
			return "", err
		}
		return canonicalDate(column.ct, text)
	case kindTime:
		text, err := asText(value)
		if err != nil {
			return "", err
		}
		return canonicalTime(text)
	case kindBinary:
		switch typed := value.(type) {
		case []byte:
			return base64.StdEncoding.EncodeToString(typed), nil
		case string:
			return base64.StdEncoding.EncodeToString([]byte(typed)), nil
		default:
			return "", fmt.Errorf("binary value has unexpected type %T", value)
		}
	case kindJSON:
		return jsonText(value)
	default:
		return "", fmt.Errorf("unsupported column kind for %s", column.ct.Raw)
	}
}

func lookupCase(row map[string]any, name string) (any, bool) {
	if value, ok := row[name]; ok {
		return value, true
	}
	for key, value := range row {
		if strings.EqualFold(key, name) {
			return value, true
		}
	}
	return nil, false
}

func asText(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		if !utf8.ValidString(typed) {
			return "", errors.New("value is not valid UTF-8 text")
		}
		return typed, nil
	case []byte:
		if !utf8.Valid(typed) {
			return "", errors.New("value is not valid UTF-8 text")
		}
		return string(typed), nil
	default:
		return "", fmt.Errorf("expected a text value, got %T", value)
	}
}

func jsonText(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		if !utf8.ValidString(typed) {
			return "", errors.New("JSON value is not valid UTF-8")
		}
		return typed, nil
	case []byte:
		if !utf8.Valid(typed) {
			return "", errors.New("JSON value is not valid UTF-8")
		}
		return string(typed), nil
	default:
		return "", fmt.Errorf("expected JSON text, got %T", value)
	}
}

func asInt64(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case uint64:
		if typed > math.MaxInt64 {
			return 0, fmt.Errorf("value %d exceeds the signed range of %s", typed, "bigint")
		}
		return int64(typed), nil
	case int:
		return int64(typed), nil
	case string:
		return strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
	case []byte:
		return strconv.ParseInt(strings.TrimSpace(string(typed)), 10, 64)
	default:
		return 0, fmt.Errorf("expected an integer value, got %T", value)
	}
}

func asUint64(value any) (uint64, error) {
	switch typed := value.(type) {
	case uint64:
		return typed, nil
	case int64:
		if typed < 0 {
			return 0, fmt.Errorf("value %d is negative for an unsigned column", typed)
		}
		return uint64(typed), nil
	case int:
		if typed < 0 {
			return 0, fmt.Errorf("value %d is negative for an unsigned column", typed)
		}
		return uint64(typed), nil
	case string:
		return strconv.ParseUint(strings.TrimSpace(typed), 10, 64)
	case []byte:
		return strconv.ParseUint(strings.TrimSpace(string(typed)), 10, 64)
	default:
		return 0, fmt.Errorf("expected an unsigned integer value, got %T", value)
	}
}

func asFloat64(value any, bits int) (float64, error) {
	switch typed := value.(type) {
	case float64:
		return typed, nil
	case float32:
		return float64(typed), nil
	case int64:
		return float64(typed), nil
	case uint64:
		return float64(typed), nil
	case json.Number:
		return strconv.ParseFloat(typed.String(), bits)
	case string:
		return strconv.ParseFloat(strings.TrimSpace(typed), bits)
	case []byte:
		return strconv.ParseFloat(strings.TrimSpace(string(typed)), bits)
	default:
		return 0, fmt.Errorf("expected a numeric value, got %T", value)
	}
}

// canonicalDecimal normalizes a decimal text to exactly scale fractional
// digits, so 1.5 and 1.50 hash to the same _id.
func canonicalDecimal(text string, scale int) (string, error) {
	value := strings.TrimSpace(text)
	if value == "" {
		return "", errors.New("empty decimal value")
	}
	negative := strings.HasPrefix(value, "-")
	if negative || strings.HasPrefix(value, "+") {
		value = value[1:]
	}
	intPart, fracPart, hasFraction := strings.Cut(value, ".")
	if intPart == "" {
		intPart = "0"
	}
	if strings.Trim(intPart, "0123456789") != "" || strings.Trim(fracPart, "0123456789") != "" {
		return "", fmt.Errorf("value %q is not a decimal number", text)
	}
	intPart = strings.TrimLeft(intPart, "0")
	if intPart == "" {
		intPart = "0"
	}
	if scale >= 0 {
		if len(fracPart) > scale {
			if strings.Trim(fracPart[scale:], "0") != "" {
				return "", fmt.Errorf("value %q has more than %d fractional digits", text, scale)
			}
			fracPart = fracPart[:scale]
		}
		fracPart += strings.Repeat("0", scale-len(fracPart))
	} else {
		fracPart = strings.TrimRight(fracPart, "0")
	}
	if intPart == "0" && strings.Trim(fracPart, "0") == "" {
		negative = false
	}
	out := intPart
	if hasFraction || fracPart != "" {
		out += "." + fracPart
	}
	if negative {
		out = "-" + out
	}
	return out, nil
}

// canonicalDate pads dates and datetimes to the fixed width used by the
// mapping, so both capture paths produce identical values and _ids.
func canonicalDate(ct columnType, text string) (string, error) {
	value := strings.TrimSpace(text)
	switch ct.Base {
	case "date":
		parsed, err := time.Parse("2006-01-02", value)
		if err != nil {
			return "", fmt.Errorf("value %q is not a valid DATE", text)
		}
		return parsed.Format("2006-01-02"), nil
	case "year":
		if len(value) != 4 {
			return "", fmt.Errorf("value %q is not a valid YEAR", text)
		}
		if _, err := strconv.Atoi(value); err != nil {
			return "", fmt.Errorf("value %q is not a valid YEAR", text)
		}
		return value, nil
	default:
		for _, layout := range []string{
			"2006-01-02 15:04:05.999999",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05.999999Z07:00",
			"2006-01-02T15:04:05Z07:00",
		} {
			parsed, err := time.Parse(layout, value)
			if err == nil {
				return parsed.Format("2006-01-02 15:04:05.000000"), nil
			}
		}
		return "", fmt.Errorf("value %q is not a valid DATETIME/TIMESTAMP", text)
	}
}

// canonicalTime keeps MySQL TIME semantics: a sign and hours above 23 are
// valid, so this cannot use time.Parse.
func canonicalTime(text string) (string, error) {
	value := strings.TrimSpace(text)
	if value == "" {
		return "", errors.New("empty TIME value")
	}
	sign := ""
	if strings.HasPrefix(value, "-") {
		sign = "-"
		value = value[1:]
	}
	whole, fraction, _ := strings.Cut(value, ".")
	parts := strings.Split(whole, ":")
	if len(parts) != 3 {
		return "", fmt.Errorf("value %q is not a valid TIME", text)
	}
	hours, err := strconv.Atoi(parts[0])
	if err != nil || hours < 0 || hours > 838 {
		return "", fmt.Errorf("value %q is not a valid TIME", text)
	}
	minutes, err := strconv.Atoi(parts[1])
	if err != nil || minutes < 0 || minutes > 59 {
		return "", fmt.Errorf("value %q is not a valid TIME", text)
	}
	seconds, err := strconv.Atoi(parts[2])
	if err != nil || seconds < 0 || seconds > 59 {
		return "", fmt.Errorf("value %q is not a valid TIME", text)
	}
	if fraction != "" && strings.Trim(fraction, "0123456789") != "" {
		return "", fmt.Errorf("value %q is not a valid TIME", text)
	}
	if len(fraction) > 6 {
		fraction = fraction[:6]
	}
	fraction += strings.Repeat("0", 6-len(fraction))
	if hours == 0 && minutes == 0 && seconds == 0 && strings.Trim(fraction, "0") == "" {
		sign = ""
	}
	return fmt.Sprintf("%s%d:%02d:%02d.%s", sign, hours, minutes, seconds, fraction), nil
}

// canonicalEnumSet maps an ENUM/SET value to its label form. It branches on the
// Go type, never on the text: the snapshot path yields labels (string/[]byte)
// while the binlog path yields numeric encodings (int64 index or bitmask). A
// numeric-looking label, e.g. enum('2','1') value "2", stays a label and is
// never read as an index or mask.
func canonicalEnumSet(ct columnType, value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return canonicalEnumSetLabel(ct, typed)
	case []byte:
		return canonicalEnumSetLabel(ct, string(typed))
	}
	code, err := enumSetCode(value)
	if err != nil {
		return "", fmt.Errorf("%s value: %w", ct.Base, err)
	}
	if ct.Base == "enum" {
		return enumLabelByIndex(ct, code)
	}
	return setLabelsByMask(ct, code)
}

// canonicalEnumSetLabel validates the label form written by the snapshot path.
func canonicalEnumSetLabel(ct columnType, text string) (string, error) {
	if ct.Base == "enum" {
		// The empty string is MySQL's blank ENUM (index 0, also the value of a
		// legal '' member), matching enumLabelByIndex(0).
		if text == "" || containsLabel(ct.Values, text) {
			return text, nil
		}
		return "", fmt.Errorf("value %q is not a label of %s", text, ct.Raw)
	}
	if text == "" {
		return "", nil
	}
	for _, member := range strings.Split(text, ",") {
		if !containsLabel(ct.Values, member) {
			return "", fmt.Errorf("value %q is not a member of %s", member, ct.Raw)
		}
	}
	return text, nil
}

// enumSetCode reinterprets a binlog integer encoding as unsigned bits, so a
// SET bitmask using the 64th member (a negative int64) decodes correctly.
func enumSetCode(value any) (uint64, error) {
	switch typed := value.(type) {
	case int64:
		return uint64(typed), nil
	case uint64:
		return typed, nil
	case int:
		return uint64(typed), nil
	default:
		return 0, fmt.Errorf("unexpected type %T", value)
	}
}

// enumLabelByIndex decodes the binlog 1-based ENUM index. Index 0 is MySQL's
// blank/invalid ENUM, which the snapshot text path also yields as "".
func enumLabelByIndex(ct columnType, index uint64) (string, error) {
	if index == 0 {
		return "", nil
	}
	if index > uint64(len(ct.Values)) {
		return "", fmt.Errorf("enum index %d is out of range for %s", index, ct.Raw)
	}
	return ct.Values[index-1], nil
}

// setLabelsByMask decodes the binlog SET bitmask into definition-ordered labels,
// matching the order the snapshot text path returns. Any bit beyond the defined
// members is a corrupt mask.
func setLabelsByMask(ct columnType, mask uint64) (string, error) {
	if members := len(ct.Values); members < 64 && mask>>uint(members) != 0 {
		return "", fmt.Errorf("set bitmask %d does not match %s", mask, ct.Raw)
	}
	var labels []string
	for i, label := range ct.Values {
		if mask&(1<<uint(i)) != 0 {
			labels = append(labels, label)
		}
	}
	return strings.Join(labels, ","), nil
}

// canonicalYear normalizes YEAR across the two capture paths: the snapshot
// yields a 4-digit text value while the binlog yields a numeric value (0 or the
// year). Both become the fixed-width text the mapping stores, so documents and
// _ids agree.
func canonicalYear(ct columnType, value any) (string, error) {
	switch value.(type) {
	case string, []byte:
		text, err := asText(value)
		if err != nil {
			return "", err
		}
		return canonicalDate(ct, text)
	}
	n, err := asInt64(value)
	if err != nil {
		return "", fmt.Errorf("year value: %w", err)
	}
	if n < 0 || n > 9999 {
		return "", fmt.Errorf("value %d is not a valid YEAR", n)
	}
	return fmt.Sprintf("%04d", n), nil
}

func containsLabel(labels []string, want string) bool {
	for _, label := range labels {
		if label == want {
			return true
		}
	}
	return false
}
