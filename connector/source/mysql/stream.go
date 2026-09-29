package mysql

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	canalschema "github.com/go-mysql-org/go-mysql/schema"

	"github.com/PyRSA/go-cdc/common/event"
	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

// canalStream is the single incremental reader. It calls RunFrom and never mysqldump.
type canalStream struct {
	canal   *canal.Canal
	ch      chan Commit
	handler *handler
}

// OpenStream builds the binlog reader. Snapshot connections stay on OpenDB.
func OpenStream(cfg definition.Source) (*canalStream, error) {
	cc := canal.NewDefaultConfig()
	cc.Addr = fmt.Sprintf("%s:%d", cfg.Hostname, cfg.Port)
	cc.User = cfg.Username
	cc.Password = cfg.Password
	cc.Flavor = "mysql"
	cc.Charset = "utf8mb4"
	cc.ServerID = cfg.ServerID
	if cc.ServerID == 0 {
		return nil, fmt.Errorf("source.server-id: required")
	}
	cc.HeartbeatPeriod = cfg.HeartbeatInterval
	cc.Dump.ExecutionPath = ""
	cc.ParseTime = false
	if loc, err := locationForZone(cfg.TimeZone); err == nil {
		cc.TimestampStringLocation = loc
	}
	if cfg.TablesInclude != "" {
		cc.IncludeTableRegex = splitPatterns(cfg.TablesInclude)
	}
	if cfg.TablesExclude != "" {
		cc.ExcludeTableRegex = splitPatterns(cfg.TablesExclude)
	}
	instance, err := canal.NewCanal(cc)
	if err != nil {
		return nil, fmt.Errorf("source.mysql: canal: %w", err)
	}
	stream := &canalStream{canal: instance, ch: make(chan Commit)}
	stream.handler = &handler{
		ch: stream.ch, canal: instance,
		include:    compilePatterns(cfg.TablesInclude),
		exclude:    compilePatterns(cfg.TablesExclude),
		skipEvents: cfg.SkipEvents,
		skipRows:   cfg.SkipRows,
	}
	instance.SetEventHandler(stream.handler)
	return stream, nil
}

// locationForZone maps source.server-time-zone to a Go location used when
// canal formats TIMESTAMP binlog values as wall-clock strings.
func locationForZone(zone string) (*time.Location, error) {
	if zone == "" || strings.EqualFold(zone, "UTC") || zone == "Z" {
		return time.UTC, nil
	}
	if strings.HasPrefix(zone, "+") || strings.HasPrefix(zone, "-") {
		sign := 1
		raw := zone
		if strings.HasPrefix(raw, "-") {
			sign = -1
			raw = raw[1:]
		} else {
			raw = raw[1:]
		}
		parts := strings.Split(raw, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("source.server-time-zone: %q", zone)
		}
		h, err1 := strconv.Atoi(parts[0])
		m, err2 := strconv.Atoi(parts[1])
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("source.server-time-zone: %q", zone)
		}
		return time.FixedZone(zone, sign*(h*3600+m*60)), nil
	}
	return time.LoadLocation(zone)
}

func compilePatterns(raw string) []*regexp.Regexp {
	var out []*regexp.Regexp
	for _, part := range splitPatterns(raw) {
		re, err := regexp.Compile(part)
		if err != nil {
			continue
		}
		out = append(out, re)
	}
	return out
}

func splitPatterns(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// ReadFrom starts one dump from pos. GTID wins when it is set.
func (s *canalStream) ReadFrom(ctx context.Context, pos checkpoint.Position) (<-chan Commit, error) {
	go func() {
		defer close(s.ch)
		var err error
		if pos.GTID != "" {
			set, parseErr := mysql.ParseGTIDSet("mysql", pos.GTID)
			if parseErr != nil {
				s.sendErr(ctx, fmt.Errorf("source.mysql: gtid: %w", parseErr))
				return
			}
			s.handler.gtid = set
			s.handler.trackGTID = true
			err = s.canal.StartFromGTID(set)
		} else {
			err = s.canal.RunFrom(mysql.Position{Name: pos.File, Pos: pos.Pos})
		}
		if err != nil && ctx.Err() == nil {
			s.sendErr(ctx, fmt.Errorf("source.mysql: binlog: %w", err))
		}
	}()
	return s.ch, nil
}

func (s *canalStream) sendErr(ctx context.Context, err error) {
	select {
	case s.ch <- Commit{Err: err}:
	case <-ctx.Done():
	}
}

// Close stops the dump connection.
func (s *canalStream) Close() error {
	if s.canal != nil {
		s.canal.Close()
	}
	return nil
}

type handler struct {
	canal.DummyEventHandler
	ch         chan Commit
	canal      *canal.Canal
	include    []*regexp.Regexp
	exclude    []*regexp.Regexp
	gtid       mysql.GTIDSet
	trackGTID  bool
	skipEvents int
	skipRows   int
}

func (h *handler) captured(database, table string) bool {
	if len(h.include) == 0 {
		return false
	}
	id := database + "." + table
	included := false
	for _, re := range h.include {
		if fullMatch(re, id) {
			included = true
			break
		}
	}
	if !included {
		return false
	}
	for _, re := range h.exclude {
		if fullMatch(re, id) {
			return false
		}
	}
	return true
}

func fullMatch(re *regexp.Regexp, value string) bool {
	loc := re.FindStringIndex(value)
	return loc != nil && loc[0] == 0 && loc[1] == len(value)
}

func (h *handler) OnRow(e *canal.RowsEvent) error {
	if e.Table == nil || !h.captured(e.Table.Schema, e.Table.Name) {
		return nil
	}
	if h.dropEvent() {
		return nil
	}
	columns := make([]string, len(e.Table.Columns))
	for i, column := range e.Table.Columns {
		columns[i] = column.Name
	}
	events, err := RowsToEvents(e.Action, e.Table.Schema, e.Table.Name, columns, e.Rows)
	if err != nil {
		return err
	}
	keys := primaryKeys(e.Table)
	for i := range events {
		if h.dropRow() {
			continue
		}
		item := events[i]
		item.Keys = append([]string(nil), keys...)
		h.ch <- Commit{Event: &item}
	}
	return nil
}

func (h *handler) OnDDL(_ *replication.EventHeader, _ mysql.Position, query *replication.QueryEvent) error {
	text := string(query.Query)
	if strings.EqualFold(text, "BEGIN") {
		return nil
	}
	change := schema.Classify(text)
	if change.Action == schema.ActionOther {
		return nil
	}
	database := change.Database
	if database == "" {
		database = string(query.Schema)
	}
	if !h.captured(database, change.Table) {
		return nil
	}
	if h.dropEvent() {
		return nil
	}
	commit := Commit{Event: &event.Event{
		Op: event.OpDDL, Database: database, Table: change.Table,
		SourceDatabase: database, SourceTable: change.Table, DDL: text,
	}}
	if change.Action == schema.ActionCreate && h.canal != nil {
		table, err := h.canal.GetTable(database, change.Table)
		if err != nil || table == nil {
			if err == nil {
				err = fmt.Errorf("table not found")
			}
			return fmt.Errorf("source.mysql: columns for %s.%s: %w", database, change.Table, err)
		}
		copied := tableFromCanal(table)
		applyCreateNullability(&copied, text)
		commit.Schema = &copied
	}
	h.ch <- commit
	return nil
}

func (h *handler) dropEvent() bool {
	if h.skipEvents > 0 {
		h.skipEvents--
		return true
	}
	return false
}

func (h *handler) dropRow() bool {
	if h.skipRows > 0 {
		h.skipRows--
		return true
	}
	return false
}

func primaryKeys(table *canalschema.Table) []string {
	if table == nil {
		return nil
	}
	var keys []string
	for i := range table.Columns {
		if table.IsPrimaryKey(i) {
			keys = append(keys, table.Columns[i].Name)
		}
	}
	return keys
}

func tableFromCanal(table *canalschema.Table) schema.Table {
	out := schema.Table{Database: table.Schema, Table: table.Name}
	for i, column := range table.Columns {
		mysqlType := column.RawType
		if mysqlType == "" {
			mysqlType = "text"
		}
		out.Columns = append(out.Columns, schema.Column{
			Name: column.Name, Type: mysqlType, Nullable: !table.IsPrimaryKey(i), AutoIncrement: column.IsAuto,
		})
		if table.IsPrimaryKey(i) {
			out.PrimaryKeys = append(out.PrimaryKeys, column.Name)
		}
	}
	return out
}

func (h *handler) OnXID(_ *replication.EventHeader, next mysql.Position) error {
	gtid := ""
	if h.trackGTID && h.gtid != nil {
		gtid = h.gtid.String()
	}
	h.ch <- Commit{Position: &checkpoint.Position{File: next.Name, Pos: next.Pos, GTID: gtid}}
	return nil
}

func (h *handler) OnGTID(_ *replication.EventHeader, gtid mysql.BinlogGTIDEvent) error {
	if gtid == nil {
		return nil
	}
	next, err := gtid.GTIDNext()
	if err != nil || next == nil {
		return err
	}
	if h.gtid == nil {
		parsed, parseErr := mysql.ParseGTIDSet("mysql", next.String())
		if parseErr != nil {
			return parseErr
		}
		h.gtid = parsed
		return nil
	}
	return h.gtid.Update(next.String())
}

func (h *handler) String() string { return "go-cdc" }

func applyCreateNullability(table *schema.Table, ddl string) {
	for i, column := range table.Columns {
		nullable, ok := nullabilityOf(ddl, column.Name)
		if ok {
			table.Columns[i].Nullable = nullable
		}
	}
}

func nullabilityOf(ddl, name string) (bool, bool) {
	lower := strings.ToLower(ddl)
	needle := strings.ToLower(name)
	rest := lower
	for {
		i := strings.Index(rest, needle)
		if i < 0 {
			return false, false
		}
		end := i + len(needle)
		if identAt(rest, i, end) {
			segment := rest[end:]
			def := segment[:cutColumnDef(segment)]
			if strings.Contains(def, "not null") {
				return false, true
			}
			if strings.Contains(def, "null") {
				return true, true
			}
			return false, false
		}
		rest = rest[end:]
	}
}

func identAt(sql string, start, end int) bool {
	if start > 0 && isIdent(sql[start-1]) {
		return false
	}
	if end < len(sql) && isIdent(sql[end]) {
		return false
	}
	return true
}

func isIdent(b byte) bool {
	return b == '_' || b == '`' || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func cutColumnDef(sql string) int {
	depth := 0
	for i := 0; i < len(sql); i++ {
		switch sql[i] {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return i
			}
			depth--
		case ',':
			if depth == 0 {
				return i
			}
		}
	}
	return len(sql)
}
