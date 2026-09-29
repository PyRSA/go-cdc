package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
	"github.com/PyRSA/go-cdc/runtime/checkpoint"
)

// sqlDB reads snapshot chunks and catalog metadata. It does not dump binlog.
type sqlDB struct {
	db     *sql.DB
	cfg    definition.Source
	types  map[string]string
	server uint32
}

// OpenDB opens the JDBC-style connection used for snapshot reads and checks ROW/FULL.
func OpenDB(cfg definition.Source) (*sqlDB, error) {
	dsn, err := sinkDSN(cfg)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, jdbcErr(cfg, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, jdbcErr(cfg, err)
	}
	if err := applyTimeZone(ctx, db, cfg.TimeZone); err != nil {
		_ = db.Close()
		return nil, err
	}
	format, image, err := readBinlogMode(ctx, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if !strings.EqualFold(format, "ROW") || !strings.EqualFold(image, "FULL") {
		_ = db.Close()
		return nil, fmt.Errorf("binlog_format=%s binlog_row_image=%s; both must be ROW and FULL", format, image)
	}
	server := cfg.ServerID
	if server == 0 {
		used, err := listReplicaServerIDs(ctx, db)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		server, err = pickServerID(used)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	handle := &sqlDB{db: db, cfg: cfg, types: map[string]string{}, server: server}
	return handle, nil
}

// ServerID is the replication id used for the binlog dump. The CREATE scan uses the next integer.
func (d *sqlDB) ServerID() uint32 { return d.server }

func pickServerID(used map[uint32]struct{}) (uint32, error) {
	for id := uint32(5400); id < 6400; id++ {
		if _, taken := used[id]; taken {
			continue
		}
		if _, taken := used[id+1]; taken {
			continue
		}
		return id, nil
	}
	return 0, fmt.Errorf("source.server-id: no free id in 5400-6400")
}

func listReplicaServerIDs(ctx context.Context, db *sql.DB) (map[uint32]struct{}, error) {
	rows, err := db.QueryContext(ctx, "SHOW SLAVE HOSTS")
	if err != nil {
		return nil, fmt.Errorf("source.server-id: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("source.server-id: %w", err)
	}
	index := -1
	for i, name := range cols {
		if strings.EqualFold(name, "server_id") {
			index = i
			break
		}
	}
	if index < 0 {
		return nil, fmt.Errorf("source.server-id: SHOW SLAVE HOSTS has no Server_id column")
	}
	used := map[uint32]struct{}{}
	for rows.Next() {
		raw := make([]sql.NullString, len(cols))
		dest := make([]any, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("source.server-id: %w", err)
		}
		if !raw[index].Valid || raw[index].String == "" {
			continue
		}
		n, err := strconv.ParseUint(raw[index].String, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("source.server-id: %w", err)
		}
		used[uint32(n)] = struct{}{}
	}
	return used, rows.Err()
}

func sinkDSN(cfg definition.Source) (string, error) {
	driver := mysqldriver.NewConfig()
	driver.User = cfg.Username
	driver.Passwd = cfg.Password
	driver.Net = "tcp"
	driver.Addr = net.JoinHostPort(cfg.Hostname, fmt.Sprintf("%d", cfg.Port))
	driver.Timeout = cfg.ConnectTimeout
	driver.ParseTime = false
	driver.Params = map[string]string{}
	for key, value := range cfg.JDBCProperties {
		driver.Params[key] = value
	}
	return driver.FormatDSN(), nil
}

func applyTimeZone(ctx context.Context, db *sql.DB, zone string) error {
	if zone == "" || strings.EqualFold(zone, "UTC") || zone == "Z" {
		zone = "+00:00"
	}
	if !safeTimeZone(zone) {
		return fmt.Errorf("source.server-time-zone: %q", zone)
	}
	if _, err := db.ExecContext(ctx, "SET time_zone = '"+zone+"'"); err != nil {
		return fmt.Errorf("source.server-time-zone: %w", err)
	}
	return nil
}

func safeTimeZone(zone string) bool {
	if zone == "" || len(zone) > 64 {
		return false
	}
	for _, r := range zone {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '/' || r == '_' || r == '+' || r == '-' || r == ':':
		default:
			return false
		}
	}
	return true
}

func jdbcErr(cfg definition.Source, err error) error {
	if len(cfg.JDBCProperties) == 0 {
		return fmt.Errorf("source.mysql: %w", err)
	}
	names := make([]string, 0, len(cfg.JDBCProperties))
	for name := range cfg.JDBCProperties {
		names = append(names, "jdbc.properties."+name)
	}
	return fmt.Errorf("source.mysql: driver rejected %s: %w", strings.Join(names, ", "), err)
}

func readBinlogMode(ctx context.Context, db *sql.DB) (string, string, error) {
	rows, err := db.QueryContext(ctx, "SHOW VARIABLES WHERE Variable_name IN ('binlog_format', 'binlog_row_image')")
	if err != nil {
		return "", "", err
	}
	defer rows.Close()
	values := map[string]string{}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return "", "", err
		}
		values[strings.ToLower(name)] = value
	}
	return values["binlog_format"], values["binlog_row_image"], rows.Err()
}

func (d *sqlDB) MasterStatus(ctx context.Context) (checkpoint.Position, error) {
	rows, err := d.db.QueryContext(ctx, "SHOW MASTER STATUS")
	if err != nil {
		return checkpoint.Position{}, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return checkpoint.Position{}, err
	}
	if !rows.Next() {
		return checkpoint.Position{}, fmt.Errorf("SHOW MASTER STATUS returned no row")
	}
	raw := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return checkpoint.Position{}, err
	}
	pos := checkpoint.Position{}
	for i, name := range cols {
		switch strings.ToLower(name) {
		case "file":
			pos.File = raw[i].String
		case "position":
			n, _ := strconv.ParseUint(raw[i].String, 10, 32)
			pos.Pos = uint32(n)
		case "executed_gtid_set":
			pos.GTID = raw[i].String
		}
	}
	return pos, rows.Err()
}

func (d *sqlDB) ListTables(ctx context.Context) ([]schema.Table, error) {
	rows, err := d.db.QueryContext(ctx, `
SELECT c.table_schema, c.table_name, c.column_name, c.column_type, c.is_nullable, c.extra, c.ordinal_position,
       k.ordinal_position
FROM information_schema.columns c
LEFT JOIN information_schema.key_column_usage k
  ON k.table_schema = c.table_schema AND k.table_name = c.table_name
 AND k.column_name = c.column_name AND k.constraint_name = 'PRIMARY'
WHERE c.table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
ORDER BY c.table_schema, c.table_name, c.ordinal_position`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tables []schema.Table
	index := map[string]int{}
	keyParts := map[string][]pkPart{}
	for rows.Next() {
		var dbName, tableName, column, colType, nullable, extra string
		var ordinal int
		var keyPos sql.NullInt64
		if err := rows.Scan(&dbName, &tableName, &column, &colType, &nullable, &extra, &ordinal, &keyPos); err != nil {
			return nil, err
		}
		id := dbName + "." + tableName
		d.types[id+"."+column] = colType
		at, ok := index[id]
		if !ok {
			tables = append(tables, schema.Table{Database: dbName, Table: tableName})
			at = len(tables) - 1
			index[id] = at
		}
		tables[at].Columns = append(tables[at].Columns, schema.Column{
			Name: column, Type: colType, Nullable: strings.EqualFold(nullable, "YES"),
			AutoIncrement: strings.Contains(strings.ToUpper(extra), "AUTO_INCREMENT"),
		})
		if keyPos.Valid {
			keyParts[id] = append(keyParts[id], pkPart{pos: int(keyPos.Int64), name: column})
		}
	}
	for i := range tables {
		id := tables[i].Database + "." + tables[i].Table
		tables[i].PrimaryKeys = primaryKeyNames(keyParts[id])
	}
	return tables, rows.Err()
}

type pkPart struct {
	pos  int
	name string
}

func primaryKeyNames(parts []pkPart) []string {
	sort.Slice(parts, func(i, j int) bool { return parts[i].pos < parts[j].pos })
	names := make([]string, len(parts))
	for i, part := range parts {
		names[i] = part.name
	}
	return names
}

func (d *sqlDB) SplitKeys(ctx context.Context, database, table, key string, low *string, limit int) ([]string, error) {
	statement, args := ChunkKeySQL(database, table, key, low, limit)
	statement = strings.Replace(statement, "SELECT "+schema.Quote(key), "SELECT CAST("+schema.Quote(key)+" AS CHAR)", 1)
	rows, err := d.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var value sql.NullString
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		keys = append(keys, value.String)
	}
	return keys, rows.Err()
}

func (d *sqlDB) ReadChunk(ctx context.Context, database, table string, columns []string, key, low string, high *string) ([]map[string]any, error) {
	fetch := d.cfg.FetchSize
	if fetch < 1 {
		fetch = 1024
	}
	var out []map[string]any
	var after any
	for {
		page, err := d.readChunkPage(ctx, database, table, columns, key, low, high, after, fetch)
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if len(page) < fetch {
			return out, nil
		}
		next, ok := chunkKeyValue(page[len(page)-1], key)
		if !ok || next == nil {
			return nil, fmt.Errorf("%s.%s: chunk key %s missing from snapshot page", database, table, key)
		}
		if after != nil && fmt.Sprint(after) == fmt.Sprint(next) {
			return nil, fmt.Errorf("%s.%s: chunk key %s did not advance", database, table, key)
		}
		after = next
	}
}

func chunkKeyValue(row map[string]any, key string) (any, bool) {
	if value, ok := row[key]; ok {
		return value, true
	}
	for name, value := range row {
		if strings.EqualFold(name, key) {
			return value, true
		}
	}
	return nil, false
}

func (d *sqlDB) readChunkPage(ctx context.Context, database, table string, columns []string, key, low string, high *string, after any, limit int) ([]map[string]any, error) {
	statement, args := ReadChunkSQL(database, table, columns, key, low, high, after, limit)
	rows, err := d.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for rows.Next() {
		raw := make([]sql.RawBytes, len(names))
		dest := make([]any, len(names))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		row := make(map[string]any, len(names))
		for i, name := range names {
			row[name] = typedValue(d.types[database+"."+table+"."+name], raw[i])
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (d *sqlDB) EarliestBinlog(ctx context.Context) (checkpoint.Position, error) {
	logs, err := d.binaryLogs(ctx)
	if err != nil {
		return checkpoint.Position{}, err
	}
	if len(logs) == 0 {
		return checkpoint.Position{}, fmt.Errorf("source.scan.startup.mode: SHOW BINARY LOGS returned no files")
	}
	return checkpoint.Position{File: logs[0], Pos: 4}, nil
}

func (d *sqlDB) PositionAt(ctx context.Context, millis int64) (checkpoint.Position, error) {
	if millis <= 0 {
		return checkpoint.Position{}, fmt.Errorf("source.scan.startup.timestamp-millis: required")
	}
	end, err := d.MasterStatus(ctx)
	if err != nil {
		return checkpoint.Position{}, err
	}
	logs, err := d.binaryLogs(ctx)
	if err != nil {
		return checkpoint.Position{}, err
	}
	if len(logs) == 0 {
		return checkpoint.Position{}, fmt.Errorf("source.scan.startup.timestamp-millis: SHOW BINARY LOGS returned no files")
	}
	target := uint32(millis / 1000)
	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: d.server + 1,
		Flavor:   "mysql",
		Host:     d.cfg.Hostname,
		Port:     uint16(d.cfg.Port),
		User:     d.cfg.Username,
		Password: d.cfg.Password,
	})
	defer syncer.Close()
	streamer, err := syncer.StartSync(mysql.Position{Name: logs[0], Pos: 4})
	if err != nil {
		return checkpoint.Position{}, fmt.Errorf("source.scan.startup.timestamp-millis: %w", err)
	}
	file := logs[0]
	start := uint32(4)
	for {
		if err := ctx.Err(); err != nil {
			return checkpoint.Position{}, err
		}
		if scanDone(file, start, end) {
			return end, nil
		}
		ev, err := streamer.GetEvent(ctx)
		if err != nil {
			return checkpoint.Position{}, fmt.Errorf("source.scan.startup.timestamp-millis: %w", err)
		}
		if rotate, ok := ev.Event.(*replication.RotateEvent); ok {
			if scanDone(file, ev.Header.LogPos, end) || binlogFileAfter(string(rotate.NextLogName), end.File) {
				return end, nil
			}
			file = string(rotate.NextLogName)
			start = uint32(rotate.Position)
			if start == 0 {
				start = 4
			}
			continue
		}
		if ev.Header.Timestamp >= target && ev.Header.Timestamp != 0 {
			return checkpoint.Position{File: file, Pos: start}, nil
		}
		if ev.Header.LogPos > 0 {
			start = ev.Header.LogPos
		}
		if scanDone(file, ev.Header.LogPos, end) {
			return end, nil
		}
	}
}

func (d *sqlDB) binaryLogs(ctx context.Context) ([]string, error) {
	rows, err := d.db.QueryContext(ctx, "SHOW BINARY LOGS")
	if err != nil {
		return nil, fmt.Errorf("source.scan.startup.mode: %w", err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("source.scan.startup.mode: %w", err)
	}
	index := -1
	for i, name := range cols {
		if strings.EqualFold(name, "log_name") {
			index = i
			break
		}
	}
	if index < 0 {
		return nil, fmt.Errorf("source.scan.startup.mode: SHOW BINARY LOGS has no Log_name column")
	}
	var names []string
	for rows.Next() {
		raw := make([]sql.NullString, len(cols))
		dest := make([]any, len(cols))
		for i := range raw {
			dest[i] = &raw[i]
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("source.scan.startup.mode: %w", err)
		}
		if raw[index].Valid && raw[index].String != "" {
			names = append(names, raw[index].String)
		}
	}
	return names, rows.Err()
}

func typedValue(mysqlType string, raw sql.RawBytes) any {
	if raw == nil {
		return nil
	}
	base := strings.ToLower(mysqlType)
	unsigned := strings.Contains(base, "unsigned")
	if i := strings.IndexByte(base, '('); i >= 0 {
		base = base[:i]
	}
	base = strings.TrimSpace(strings.TrimSuffix(base, "unsigned"))
	text := string(raw)
	switch base {
	case "tinyint", "smallint", "int", "integer", "bigint", "mediumint":
		if unsigned {
			n, err := strconv.ParseUint(text, 10, 64)
			if err == nil {
				return n
			}
		}
		n, err := strconv.ParseInt(text, 10, 64)
		if err == nil {
			return n
		}
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob":
		return append([]byte(nil), raw...)
	}
	return text
}

func scanDone(currentFile string, logPos uint32, end checkpoint.Position) bool {
	if end.File == "" || currentFile == "" {
		return false
	}
	if binlogFileAfter(currentFile, end.File) {
		return true
	}
	return currentFile == end.File && end.Pos > 0 && logPos >= end.Pos
}

func binlogFileAfter(file, end string) bool {
	if file == "" || end == "" || file == end {
		return false
	}
	fileN, fileOK := binlogIndex(file)
	endN, endOK := binlogIndex(end)
	if fileOK && endOK {
		return fileN > endN
	}
	return file > end
}

func binlogIndex(file string) (int, bool) {
	i := strings.LastIndex(file, ".")
	if i < 0 || i == len(file)-1 {
		return 0, false
	}
	n, err := strconv.Atoi(file[i+1:])
	if err != nil {
		return 0, false
	}
	return n, true
}

func (d *sqlDB) Close() error { return d.db.Close() }
