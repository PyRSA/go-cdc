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
	logged := ""
	for {
		row, ok := findLoadRow(t, db, filtered, label, true)
		if !ok {
			row, ok = findLoadRow(t, db, unfiltered, label, false)
		}
		if ok {
			last = row
			seen := row["state"] + "/" + row["jobid"]
			if seen != logged {
				logged = seen
				t.Logf("SHOW LOAD label=%s state=%s jobid=%s type=%s loadstarttime=%s etlstarttime=%s errormsg=%q",
					label, row["state"], row["jobid"], row["type"], row["loadstarttime"], row["etlstarttime"], row["errormsg"])
			}
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

// loadRows runs one SHOW LOAD statement and returns only the rows whose Label
// matches, together with the number of rows Doris actually returned. The
// returned count makes WHERE effectiveness observable: a filtered query that
// returns unrelated jobs proves the build ignored the filter (the sink
// validates every row for exactly that reason).
func loadRows(t *testing.T, db *sql.DB, query, label string, exactFilter bool) (matched []map[string]string, returned int) {
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
	ignored := 0
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
		returned++
		if !strings.EqualFold(row["label"], label) {
			ignored++
			continue
		}
		matched = append(matched, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("show load rows: %v", err)
	}
	if exactFilter && ignored > 0 {
		t.Logf("SHOW LOAD WHERE Label=%q returned %d unrelated row(s) out of %d: Doris ignored the filter; the sink validates every row anyway",
			label, ignored, returned)
	}
	return matched, returned
}

func filteredLoadRows(t *testing.T, db *sql.DB, database, label string) ([]map[string]string, int) {
	t.Helper()
	query := "SHOW LOAD FROM " + schema.Quote(database) + " WHERE Label = '" + label + "'"
	return loadRows(t, db, query, label, true)
}

// findLoadRow returns the only row for label. A second row is a hard failure:
// the sink refuses to guess in that case (client_sql.showLoadQuery) and a test
// helper that silently kept the last row would hide exactly the ambiguity M-4
// is about.
func findLoadRow(t *testing.T, db *sql.DB, query, label string, exactFilter bool) (map[string]string, bool) {
	t.Helper()
	matched, _ := loadRows(t, db, query, label, exactFilter)
	switch len(matched) {
	case 0:
		return nil, false
	case 1:
		return matched[0], true
	default:
		t.Fatalf("SHOW LOAD returned %d rows for label %q; the sink treats a duplicate label as fatal: %v",
			len(matched), label, matched)
		return nil, false
	}
}

// assertSingleLoadRow reports how many rows SHOW LOAD exposes for one label.
// Zero rows is logged rather than failed (no ambiguity is possible then, and
// the sink retries the same label); anything above one fails, because that is
// the condition the sink refuses to resolve by row order.
func assertSingleLoadRow(t *testing.T, db *sql.DB, database, label string) map[string]string {
	t.Helper()
	filtered, filteredReturned := filteredLoadRows(t, db, database, label)
	unfiltered := "SHOW LOAD FROM " + schema.Quote(database)
	scanned, scannedReturned := loadRows(t, db, unfiltered, label, false)
	t.Logf("SHOW LOAD label=%s: filtered rows=%d (returned %d), full scan rows=%d (scanned %d)",
		label, len(filtered), filteredReturned, len(scanned), scannedReturned)
	if len(filtered) > 1 {
		t.Fatalf("SHOW LOAD WHERE Label=%q returned %d rows; the sink treats a duplicate label as fatal: %v",
			label, len(filtered), filtered)
	}
	if len(scanned) > 1 {
		t.Fatalf("SHOW LOAD FROM %s returned %d rows for label %q; the sink treats a duplicate label as fatal: %v",
			database, len(scanned), label, scanned)
	}
	if len(filtered) == 0 && len(scanned) == 1 {
		t.Logf("SHOW LOAD WHERE Label=%q returned no rows; the sink falls back to a full scan, which found the one row", label)
	}
	if len(scanned) == 0 {
		t.Logf("SHOW LOAD exposes no row for label %q; duplicate-label ambiguity cannot occur, but the sink cannot confirm this load's state", label)
		return nil
	}
	return scanned[0]
}

func dorisVersion(t *testing.T, db *sql.DB) string {
	t.Helper()
	if row, ok, err := queryOptionalRow(db, "SHOW FRONTENDS"); err == nil && ok && row["version"] != "" {
		return row["version"]
	}
	if row, ok, err := queryOptionalRow(db, "SELECT VERSION() AS version"); err == nil && ok && row["version"] != "" {
		return row["version"]
	}
	return "unavailable"
}

// TestIntegrationShowLoadSingleRowPerLabel is the P0 probe for M-4: one
// Stream Load must surface as exactly one SHOW LOAD row, the label filter must
// not leak another job, and a retry with the same label must not add a second
// row. Two labels in one database make an ignored WHERE clause observable.
func TestIntegrationShowLoadSingleRowPerLabel(t *testing.T) {
	cfg := integrationConfig(t)
	db := openDB(t, cfg)
	t.Logf("Doris FE version: %s", dorisVersion(t, db))
	database := uniqueName("go_cdc_it")
	createDatabase(t, db, database)
	const table = "load_labels"

	sk := openSink(t, cfg)
	if err := sk.CreateTable(ordersTable(database, table)); err != nil {
		t.Fatalf("create %s: %v", table, err)
	}

	stamp := time.Now().UnixNano()
	labels := []string{
		fmt.Sprintf("go_cdc_it_%d_a", stamp),
		fmt.Sprintf("go_cdc_it_%d_b", stamp),
	}
	for i, label := range labels {
		body := []byte(fmt.Sprintf(`{"id":%d,"name":"row-%d"}`, i+1, i+1) + "\n")
		resp := rawStreamLoad(t, cfg, database, table, label, body)
		if resp.Status != "Success" {
			t.Fatalf("stream load %s status=%q message=%q", label, resp.Status, resp.Message)
		}
		if resp.Label != "" && resp.Label != label {
			t.Fatalf("stream load %s response Label=%q", label, resp.Label)
		}
	}

	// Retrying a label that already finished is the one path that can add a
	// second SHOW LOAD row for the same label. Production treats that as fatal,
	// so it must stay at one row here. This runs outside the SHOW LOAD subtest:
	// a duplicate label must never append rows even when the build records no
	// load job at all.
	dup := rawStreamLoad(t, cfg, database, table, labels[0], []byte(`{"id":1,"name":"row-1"}`+"\n"))
	t.Logf("duplicate label %s response: status=%q existing_job_status=%q", labels[0], dup.Status, dup.ExistingJobStatus)
	if dup.Status == "Label Already Exists" && dup.ExistingJobStatus != "" &&
		!strings.EqualFold(dup.ExistingJobStatus, "FINISHED") {
		t.Fatalf("duplicate label %s ExistingJobStatus=%q", labels[0], dup.ExistingJobStatus)
	}

	if got := queryCount(t, db, database, table); got != 2 {
		t.Fatalf("row count=%d want 2 (a duplicate label must not append rows)", got)
	}

	// The row-shape assertions live in their own subtest so a build that records
	// no Stream Load job reports SKIP for this one probe only; the duplicate-label
	// and row-count guarantees above still pass on their own.
	t.Run("each label owns exactly one SHOW LOAD row", func(t *testing.T) {
		// labels[0] was retried above, so its single row here also proves the
		// retry did not append a second SHOW LOAD job.
		observed := 0
		for _, label := range labels {
			state := waitForLoadState(t, db, database, label)
			if state == nil {
				t.Logf("SHOW LOAD never exposed label %s; skipping row assertions for it", label)
				continue
			}
			row := assertSingleLoadRow(t, db, database, label)
			if row == nil {
				continue
			}
			observed++
			if row["label"] != label {
				t.Fatalf("SHOW LOAD row label=%q want %q", row["label"], label)
			}
			if row["jobid"] == "" {
				t.Fatalf("SHOW LOAD row for %s has no JobId: %v", label, row)
			}
			if row["state"] == "" {
				t.Fatalf("SHOW LOAD row for %s has no State: %v", label, row)
			}
		}
		if observed == 0 {
			// A build that records no Stream Load job cannot exhibit the duplicate
			// label ambiguity the sink guards against. Report this probe as skipped
			// instead of green, so CI does not claim coverage it never had.
			t.Skipf("SHOW LOAD exposed none of %d Stream Load labels: the single-row and WHERE assertions could not execute on this Doris build", len(labels))
		}
	})
}
