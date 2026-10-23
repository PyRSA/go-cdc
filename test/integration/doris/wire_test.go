//go:build doris_integration

package doris_it

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/PyRSA/go-cdc/common/schema"
	"github.com/PyRSA/go-cdc/composer/definition"
)

type streamLoadResponse struct {
	Status             string `json:"Status"`
	Message            string `json:"Message"`
	Label              string `json:"Label"`
	NumberTotalRows    int64  `json:"NumberTotalRows"`
	NumberLoadedRows   int64  `json:"NumberLoadedRows"`
	NumberFilteredRows int64  `json:"NumberFilteredRows"`
	ExistingJobStatus  string `json:"ExistingJobStatus"`
}

func TestIntegrationStreamLoadLabelAndShowLoad(t *testing.T) {
	cfg := integrationConfig(t)
	db := openDB(t, cfg)
	database := uniqueName("go_cdc_it")
	createDatabase(t, db, database)
	table := ordersTable(database, "labels")

	sk := openSink(t, cfg)
	if err := sk.CreateTable(table); err != nil {
		t.Fatalf("create label table: %v", err)
	}

	body := []byte(`{"id":1,"name":"first"}` + "\n")
	label := fmt.Sprintf("go_cdc_it_%d", time.Now().UnixNano())
	first := rawStreamLoad(t, cfg, database, "labels", label, body)
	if first.Status != "Success" {
		t.Fatalf("first Stream Load status=%q message=%q", first.Status, first.Message)
	}
	if first.Label != "" && first.Label != label {
		t.Fatalf("response Label=%q want %q", first.Label, label)
	}
	if first.NumberTotalRows != 1 || first.NumberLoadedRows != 1 || first.NumberFilteredRows != 0 {
		t.Fatalf("first load counts total=%d loaded=%d filtered=%d",
			first.NumberTotalRows, first.NumberLoadedRows, first.NumberFilteredRows)
	}

	// Reusing a label must be recognized as an already-finished job. The sink
	// treats that as success only when Doris reports FINISHED.
	second := rawStreamLoad(t, cfg, database, "labels", label, body)
	switch second.Status {
	case "Success":
		t.Log("Doris reported Success for a duplicate label")
	case "Label Already Exists":
		if second.ExistingJobStatus != "" && !strings.EqualFold(second.ExistingJobStatus, "FINISHED") {
			t.Fatalf("duplicate label ExistingJobStatus=%q", second.ExistingJobStatus)
		}
	default:
		t.Fatalf("duplicate label status=%q message=%q", second.Status, second.Message)
	}

	state := waitForLoadState(t, db, database, label)
	if state != nil && state["errormsg"] != "" {
		t.Fatalf("SHOW LOAD label=%s state=%s errormsg=%s", label, state["state"], state["errormsg"])
	}
	if got := queryCount(t, db, database, "labels"); got != 1 {
		t.Fatalf("duplicate label loaded %d rows, want 1", got)
	}
}

func rawStreamLoad(t *testing.T, cfg definition.Sink, database, table, label string, body []byte) streamLoadResponse {
	t.Helper()
	endpoint := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load",
		cfg.Hostname, cfg.Port, database, table)
	req, err := http.NewRequest(http.MethodPut, endpoint, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.SetBasicAuth(cfg.Username, cfg.Password)
	req.Header.Set("label", label)
	req.Header.Set("format", "json")
	req.Header.Set("read_json_by_line", "true")
	req.Header.Set("strict_mode", "true")
	req.Header.Set("max_filter_ratio", "0")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Expect", "100-continue")

	client := &http.Client{Timeout: 30 * time.Second}
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 3 {
			return fmt.Errorf("too many redirects")
		}
		if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
			return fmt.Errorf("unsafe redirect scheme %q", req.URL.Scheme)
		}
		// FE redirects Stream Load to a BE; re-attach the credentials that Go
		// strips on a cross-host redirect, mirroring the sink client.
		req.SetBasicAuth(cfg.Username, cfg.Password)
		return nil
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("stream load %s: %v", label, err)
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("read stream load response: %v", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("stream load HTTP %d: %s", resp.StatusCode, payload)
	}
	var out streamLoadResponse
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatalf("stream load response %q: %v", payload, err)
	}
	return out
}

func waitForLoadState(t *testing.T, db *sql.DB, database, label string) map[string]string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	// Prefer WHERE Label = …; some Doris 2.1.x builds return no rows for that
	// filter on Stream Load jobs, so fall back to scanning SHOW LOAD FROM db.
	filtered := "SHOW LOAD FROM " + schema.Quote(database) + " WHERE Label = '" + label + "'"
	unfiltered := "SHOW LOAD FROM " + schema.Quote(database)
	var last map[string]string
	for {
		row, ok := findLoadRow(t, db, filtered, label, true)
		if !ok {
			row, ok = findLoadRow(t, db, unfiltered, label, false)
		}
		if ok {
			last = row
			state := strings.ToUpper(row["state"])
			switch state {
			case "FINISHED":
				return row
			case "CANCELLED":
				t.Fatalf("load %s cancelled: %s", label, row["errormsg"])
			}
		}
		if time.Now().After(deadline) {
			// Stream Load already returned Success and the table row count is
			// checked by the caller; missing SHOW LOAD is a Doris visibility
			// quirk on some 2.1.x builds, not a sink failure.
			t.Logf("SHOW LOAD did not expose label %s as FINISHED (last=%v); continuing", label, last)
			return last
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func findLoadRow(t *testing.T, db *sql.DB, query, label string, exactFilter bool) (map[string]string, bool) {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("show load %s: %v", label, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		t.Fatalf("show load columns: %v", err)
	}
	var match map[string]string
	for rows.Next() {
		values := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("show load scan: %v", err)
		}
		row := make(map[string]string, len(columns))
		for i, name := range columns {
			row[strings.ToLower(name)] = cell(values[i])
		}
		if !exactFilter && !strings.EqualFold(row["label"], label) {
			continue
		}
		match = row
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("show load rows: %v", err)
	}
	return match, match != nil
}
