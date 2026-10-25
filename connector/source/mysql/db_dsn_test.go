package mysql

import (
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/PyRSA/go-cdc/composer/definition"
)

// The snapshot connection pool must start every connection in
// source.server-time-zone, otherwise a read on a second connection renders
// TIMESTAMP in the server default zone and disagrees with the binlog path.
func TestSinkDSNPinsTimeZoneOnEveryConnection(t *testing.T) {
	cases := map[string]string{
		"":              "+00:00",
		"UTC":           "+00:00",
		"Z":             "+00:00",
		"+08:00":        "+08:00",
		"Asia/Shanghai": "Asia/Shanghai",
	}
	for zone, want := range cases {
		cfg := definition.Source{
			Hostname: "127.0.0.1", Port: 3306, Username: "u", Password: "p",
			ConnectTimeout: 5 * time.Second, TimeZone: zone,
			JDBCProperties: map[string]string{"charset": "utf8mb4"},
		}
		dsn, err := sinkDSN(cfg)
		if err != nil {
			t.Fatalf("sinkDSN(%q): %v", zone, err)
		}
		parsed, err := mysqldriver.ParseDSN(dsn)
		if err != nil {
			t.Fatalf("ParseDSN(%q): %v", dsn, err)
		}
		// Quoted so the driver sends SET time_zone = '<zone>' per connection.
		if parsed.Params["time_zone"] != "'"+want+"'" {
			t.Fatalf("zone %q: time_zone param = %s, want '%s'", zone, parsed.Params["time_zone"], want)
		}
		if parsed.Params["charset"] != "utf8mb4" {
			t.Fatalf("zone %q: configured jdbc property was dropped", zone)
		}
	}
}

func TestSinkDSNRejectsUnsafeTimeZone(t *testing.T) {
	cfg := definition.Source{
		Hostname: "127.0.0.1", Port: 3306, Username: "u", Password: "p",
		ConnectTimeout: 5 * time.Second, TimeZone: "UTC'; DROP TABLE t; --",
	}
	if _, err := sinkDSN(cfg); err == nil {
		t.Fatal("an injectable time zone was accepted")
	}
}
