# Elasticsearch Sink Connector

Writes each change as one document in **Elasticsearch 7, 8 or 9** using the
official client
[`github.com/elastic/go-elasticsearch/v7`](https://github.com/elastic/go-elasticsearch)
, [`v8`](https://github.com/elastic/go-elasticsearch) or
[`v9`](https://github.com/elastic/go-elasticsearch), selected by
`sink.version`.

**YAML:** top-level `sink` with `type: elasticsearch`.

## Version & Dependencies

| Item | Support |
| --- | --- |
| Elasticsearch | **7.17.x** and **8.x** (verified) and **9.5.x** (targeted by the ES9 CI job). 7.0–7.16 are not verified |
| Client | Official `go-elasticsearch/v7` (v7.17.x), `go-elasticsearch/v8` (v8.19.x) or `go-elasticsearch/v9` (v9.5.x), selected by `version` |
| Mapping / bulk format | Typeless mappings and the same NDJSON bulk body for all three versions |
| SDK retries | **Disabled** (`elastictransport.WithDisableRetry()`); the sink owns its bounded retry loop |
| Compatibility mode | **Off**; the configured version selects the client, never the server |
| Packaging | Shipped inside the `go-cdc` binary; no CGo |

The sink business layer only talks to a small `client.Client` interface
(`Ping`, `IndexExists`, `CreateIndex`, `GetMapping`, `Bulk`, `Close`); SDK
request/response types stay inside `client/v7`, `client/v8` and `client/v9`.
Index naming, mapping derivation, `_id` hashing, bulk planning, retry
classification, and checkpoint behaviour are shared by all versions.

## Choosing the server version

```yaml
sink:
  type: elasticsearch
  version: 9          # 7 → v7 client, 8 → v8 client, 9 → v9 client
```

- **Unset** (or `0` in a programmatic config) → `8`, so existing pipelines are
  unchanged.
- `7` → the official v7 client, for an Elasticsearch 7.x cluster.
- `8` → the official v8 client.
- `9` → the official v9 client, for an Elasticsearch 9.x cluster.
- Anything else (`6`, `10`, `7.5`, `v7`, …) fails at parse time with
  `sink.version`; the sink never falls back to another client.
- The cluster version is **never probed**: a pipeline always uses the client it
  was configured with, even when it points at a different server version.
- Every client requires the `X-Elastic-Product` response header, which 7.14+
  and all 8.x/9.x releases send. Targeting an older 7.x server is therefore not
  supported.

## Delivery semantics

`Write` sends the whole runner batch as **one or more bulk requests** and
returns an error unless **every item** was acknowledged. Nothing is buffered, so
`Flush` is a no-op and a `nil` error means Elasticsearch already accepted the
documents. The runner saves the checkpoint only after `Write`+`Flush` succeed,
so a rejected document never advances the checkpoint.

Each bulk attempt is bounded by `request-timeout`, and each **request** (its
attempts plus backoff) gets its own budget of
`(max-retries+1) * request-timeout` plus the retry budget. Because a large
`Write` is split into several requests, its total time is bounded by that
budget multiplied by the number of requests it needed, so a later request never
inherits an exhausted deadline and no request waits without a bound. Replays
are idempotent because every operation carries a deterministic `_id` (see
below).

A request body is capped by `sink.bulk-max-bytes` (default 5 MiB; `0` selects
the default). The sink encodes each operation to its final NDJSON form once —
action line, document line, and both newlines — and starts a new request when
adding the next operation would exceed the budget, so the bytes that are
measured are exactly the bytes that are sent. `write-batch-size` (and a
snapshot chunk) still cap the number of operations handed to one `Write`;
splitting only makes a request smaller, never larger, and never reorders
operations.

An operation that alone exceeds the budget cannot be split: the write fails
before anything is sent, naming the operation, its encoded size, and the limit.
Elasticsearch would reject such a body with `413` anyway, and no retry can make
it fit. Raise `bulk-max-bytes` (at most `104857600`) in that case, or shrink the
row.

## Example

```yaml
sink:
  type: elasticsearch
  version: 9                 # 7, 8 or 9; omit for 8
  hosts:
    - http://127.0.0.1:9200
  # username: elastic     # Basic auth; username and password must be set together
  # password: secret
  auto-create: true
  request-timeout: 30s
  write-batch-size: 1000
  write-batch-interval: 1s
  max-retries: 3
  # bulk-max-bytes: 5242880   # encoded bytes per bulk request (default 5 MiB)

route:
  - source-table: demo_db\.users
    sink-table: es.users
```

Full job: [`configs/demo-elasticsearch.yaml`](../../configs/demo-elasticsearch.yaml).

## Options

| Option | Required | Default | Type | Description |
| --- | --- | --- | --- | --- |
| `type` | yes | — | String | Must be `elasticsearch` |
| `version` | no | `8` | Int | Target major version: `7`, `8` or `9`. Any other value is rejected; the client is never probed from the server |
| `hosts` | yes | — | String or list | `http(s)://host:port` entries. Credentials embedded in a URL are removed from error messages |
| `username` | no | — | String | Basic auth user; must be paired with `password` |
| `password` | no | — | String | Basic auth password; must be paired with `username` |
| `auto-create` | no | **`false`** | Boolean | Create the index when it is missing. `false` + missing index fails startup |
| `index-name` | no | — | String | Explicit index for **every** routed table; overrides the derived name |
| `mapping-file` | no | — | Path | JSON object used as the Create Index body (`settings` / `mappings` / `aliases`). Missing fields are derived from the schema |
| `request-timeout` | no | `30s` | Duration | Timeout of one Elasticsearch request |
| `bulk-max-bytes` | no | `5242880` (5 MiB) | Int | Maximum encoded NDJSON bytes of one bulk request: `0` (use the default) or `1024..104857600`. A single operation above it fails the write instead of being sent |
| `write-batch-size` | no | `1000` | Int | Shared runner option: flush the DML batch when the size is reached |
| `write-batch-interval` | no | `1s` | Duration | Shared runner option: flush the DML batch on this interval |
| `max-retries` | no | `3` | Int | Retries for **transient** bulk failures (`>= 0`) |

`sink.auto-create-table` belongs to the MySQL sink and is rejected for
Elasticsearch: index creation is controlled by `sink.auto-create` only. ES-only
keys (`hosts`, `version`, `index-name`, `mapping-file`, `request-timeout`,
`bulk-max-bytes`, `auto-create`) are rejected for other sink types.

## Startup, catalog, and `auto-create`

Before the first event the runner asks the sink to prepare every routed table,
using the table list read from the source catalog filtered by `source.tables`
plus the tables already recorded in `checkpoint.captured_tables`. This covers
all three startup paths: snapshot, checkpoint resume, and binlog-only start
(where the source emits no `Schema` message). A later `CREATE` message
prepares the new table the same way.

For each target table the sink:

1. requires a primary key and at least one column,
2. resolves and validates the index name,
3. derives the mapping and validates it against `mapping-file`,
4. checks `IndexExists`,
5. creates the index **only when `auto-create: true`**, then
6. reads the stored mapping with `GetMapping` and re-checks compatibility.

Consequences:

- `auto-create: false` and a missing index → startup fails with
  `index "..." for table db.t does not exist and sink.auto-create is false`.
- An existing index is **never** modified: no mapping update, no reindex, no
  delete. It is still compatibility-checked against the schema.
- Any catalog, mapping, or connection error fails the job before any event is
  written, so no checkpoint can move past an unprepared table.
- Preparing the same table again with an identical schema is skipped; a
  changed schema is re-verified instead of trusting the cached plan.

## Index naming

1. `sink.index-name`, when set, is used verbatim for every target table.
2. Otherwise the index is `normalizeIndexName(db + "." + table)` of the
   **routed** target table:
   - upper case is lower-cased;
   - characters other than `a-z 0-9 . _ -` become `_`;
   - a leading `_`, `-`, or `+` gets a `cdc` prefix;
   - if normalization changed the name, `-<12 hex of sha256(raw)>` is appended
     so case- or symbol-different tables cannot collapse into one index;
   - names longer than 255 bytes are truncated and keep the hash suffix;
   - an empty result becomes `cdc`.

Route therefore changes the index: routing `demo_db.users` to `es.users`
writes to `es.users` (or to `sink.index-name` when it is set). The `_id` scope
uses the routed name as well.

## Index name / mapping file combinations

| `index-name` | `mapping-file` | Behaviour |
| --- | --- | --- |
| — | — | One derived index per routed table, mapping fully derived from the schema |
| set | — | Every table writes to that index; all tables must be mutually compatible |
| — | set | Derived index per table; the file is merged over the derived mapping |
| set | set | One explicit index; the file is merged over the derived mapping |

`mapping-file` is a Create Index body:

```json
{
  "settings": {"number_of_shards": 1},
  "mappings": {
    "dynamic": "strict",
    "properties": {
      "id": {"type": "unsigned_long"},
      "amount": {"type": "keyword"}
    }
  }
}
```

Rules enforced while building the body:

- `mapping` (singular) is rejected — use `mappings`.
- Top-level `properties` is rejected — nest it under `mappings`.
- `mappings.mappings` (a double nesting produced by copying a Get Mapping
  response) is rejected.
- `mappings.dynamic` must be a string or boolean.
- File fields win; every column the file does not define is derived from the
  schema.

## Schema → mapping

| MySQL type | Elasticsearch type |
| --- | --- |
| `bool`, `boolean`, `tinyint(1)` | `byte` |
| `tinyint` / `tinyint unsigned` | `byte` / `short` |
| `smallint` / `smallint unsigned` | `short` / `integer` |
| `mediumint` | `integer` |
| `int` / `int unsigned` | `integer` / `long` |
| `bigint` / `bigint unsigned` | `long` / `unsigned_long` |
| `float` | `float` |
| `double`, `real` | `double` |
| `decimal`, `numeric`, `dec`, `fixed` | **`keyword`** |
| `date` | `date` + `yyyy-MM-dd` |
| `datetime`, `timestamp` | `date` + `yyyy-MM-dd HH:mm:ss.SSSSSS` |
| `year` | `date` + `yyyy` |
| `time` | `keyword` |
| `char`, `varchar` (`<= 512` chars) | `keyword` |
| `char`, `varchar` (`> 512` chars), `*text` | `text` |
| `json` | `object` with `"dynamic": true` |
| `binary`, `varbinary`, `*blob` | `binary` (base64 in JSON) |
| `enum`, `set` | `keyword` |

**Not supported** (hard failure naming table and column, never a silent
fallback to `text`): `bit`, `geometry`/`point`/`polygon` and any other unknown
base type, and unknown type modifiers.

### Special value handling

- **DECIMAL/NUMERIC** is stored as canonical text at the column scale
  (`1.5` in `decimal(10,2)` becomes `1.50`, `-0.00` becomes `0.00`), so the
  full precision round-trips and both capture paths hash identically. An
  explicit mapping therefore has to be `keyword`; `double`/`scaled_float` is
  rejected.
- **TIMESTAMP/DATETIME** is written in the source session time zone
  (`source.server-time-zone`) with **no time-zone conversion**, padded to
  `yyyy-MM-dd HH:mm:ss.SSSSSS`.
- **DATE** is padded to `yyyy-MM-dd`; **YEAR** stays a 4-digit string;
  **TIME** keeps MySQL semantics (sign, hours up to 838) as
  `[-]H:MM:SS.ffffff` text.
- **JSON** must be valid JSON text; it is compacted (key order preserved) and
  written as an `object`. Invalid JSON fails the write.
- **BLOB/BINARY** is base64-encoded and needs a `binary` field.
- **ENUM** accepts a label or a 1-based index; **SET** accepts a label list
  (as produced by the source) or a numeric bitmask. Both are written as the
  label text.
- `NULL` is written as JSON `null`; a column missing from the event fails the
  write (projection and `primary-keys` must agree).

## Primary keys and `_id`

Every table must have a primary key; a table without one fails at catalog
time and an event whose key is NULL or missing fails at write time. v1 never
lets Elasticsearch assign `_id`.

`_id` is `sha256` over the **routed** `database.table` scope followed by each
final-schema primary key in schema order, each encoded as
`4-byte big-endian length | text`, where the text is the canonical value form
above. The 64-character hex digest is the document id. Properties:

- `event.Keys` order is never used; the final `schema.Table.PrimaryKeys` order
  is authoritative. An event whose `Keys` disagree with the schema fails.
- Snapshot and binlog values produce the same id (`uint64(7)`, `int64(7)`,
  and `"7"` all hash as `7`).
- Composite keys are unambiguous (`("ab","c")` differs from `("a","bc")`).
- Two tables sharing one index keep distinct ids because the routed table name
  is part of the hash.

## Operations

| Source operation | Elasticsearch action |
| --- | --- |
| Snapshot read (`r`), `Create` (`c`), `Update` (`u`) | `index` with the full `after` document |
| `Delete` (`d`) | `delete` with the same `_id` |

`Update` always writes the complete `after` row (no partial update, no
script). `Delete` uses the `before` row only for the key. Any other operation
(DDL, truncate, unknown) is rejected with the operation name, the capturing
table, and the routed table.

## DDL and schema changes

v1 has **no schema evolution**: `ApplySchema` always fails, and a `CREATE`
without catalog metadata is not turned into an invented mapping. Adding,
dropping, renaming, or retyping a column therefore stops the job instead of
silently diverging from the index mapping. Recreate the index (or remove the
DDL from the pipeline) and restart from a safe position.

## Bulk errors and retries

One `Write` issues one or more bulk requests (split by `bulk-max-bytes`, sent in
order); **every item** of every request is inspected even when HTTP is 200:

- `2xx` → success.
- `delete` + `404` without an error type → success (the document is already
  gone; replays stay idempotent).
- `429`, `500`, `502`, `503`, `504`, and transport failures (no HTTP status)
  → transient: the item is retried up to `max-retries` times.
- Anything else (`400`, `401`, `403`, `404` on index, `413` for a body the
  cluster refuses, mapping/parsing errors, …) → permanent: the write fails
  immediately without retrying. A `413` is never retried: the same body cannot
  fit a second time.

Retries resend **only the failed items of the request that failed**, with an
exponential backoff starting at `100ms` and capped at `2s`, interrupted by
context cancellation. When the budget is exhausted the write fails with
`elasticsearch: bulk: N of M item(s) failed after K attempt(s)` followed by up
to 5 representative failures (action, index, id, status, error type, reason
truncated to 200 characters) and `and X more`. No document content, password,
or `Authorization` header is logged. When a `Write` was split, the error names
the request: `elasticsearch: bulk: request i/n: ...`.

Known limitation (fix tracked as a separate data-correctness task): if one
request contains the same `_id` twice and only the earlier operation fails
transiently, the retry resends that earlier value and can overwrite the value
the later operation already stored. Operations separated by a request boundary
are not affected, because requests are sent in order and a request is retried
before the next one is built.

## Existing mapping compatibility

Checked for created, existing, and shared indexes (after creation the stored
mapping is read back and re-checked):

- every schema column must exist in the mapping — v1 never adds fields;
- integer columns need a field wide enough for the full MySQL range, and
  signed values are rejected for `unsigned_long`;
- `float` accepts `float`/`double`, `double` needs `double`;
- decimal needs `keyword`, enum/set and `time` need `keyword`;
- string needs `keyword` or `text`;
- date columns need `date` **and** the exact format the sink writes
  (`yyyy-MM-dd`, `yyyy-MM-dd HH:mm:ss.SSSSSS`, or `yyyy`);
- JSON needs `object`; under root `dynamic: strict`/`false` the object field
  must set `"dynamic": true`;
- binary needs `binary`;
- extra mapping fields that no column uses are allowed.

When several tables share one index, every table is checked on its own, so an
incompatibility is reported even if another table already validated.

## Diagnostics and credentials

- Errors name the operation, the capturing table, the routed table, and the
  index; connection errors name the configured hosts with any embedded
  `user:password@` removed.
- Ping runs right after the client is created, so DNS, TLS, auth, and cluster
  problems surface at startup.
- `Close` reports transport errors; the command exits non-zero when the sink
  cannot be closed cleanly.

## Tests

Four layers, mirroring the MySQL sink suites. All of them live under
`tests/integration/` and share the harness conventions documented in
[tests/integration/README.md](../../tests/integration/README.md).

```bash
# 1. unit tests (fake client; no cluster required) - the default `go test` path
go test ./connector/sink/elasticsearch/... ./composer/ -count=1

# whole repository (still no cluster: real-cluster tests need a build tag)
go test ./...

# 2. Integration: Go sink/runner against a real ES8 (build tag `esintegration`)
# `make it-es` with no SUITES runs the full default list (go,func,load,scale)
# against a compose stack of MySQL + Elasticsearch 8.15 (security off); pin
# SUITES to trim it. `make it-es SUITES=go` runs only the Go IT.
make it-es SUITES=go    # Go IT only (-tags esintegration), not func/load/scale

# 2b. Integration of the v7 adapter against a real ES7.17 (same build tag)
make it-es7             # setup + go TestElasticsearch7* + teardown, in one shot

# 2c. Integration of the v9 adapter against a real ES9.5 (same build tag)
make it-es9             # setup + go TestElasticsearch9* + teardown, in one shot

# 3. E2E: MySQL binlog -> runner -> ES8 (snapshot, DML, route/transform,
#    checkpoint restart, mapping conflict, consistency)
make it-es SUITES=go,func   # Go IT + E2E func in one setup

# 4. Load: seeded 50k rows/table, mixed DML, batch sweep, peak writers
make it-es PROFILE=heavy SUITES=func,load

# 5. Scale: normal is CI-sized, heavy is an explicit stress run
make it-es SUITES=go,func,scale
make it-es PROFILE=heavy     # 10 DBs / 50k rows per hotspot / 45s peak
```

What each layer proves:

| Layer | Needs | Checks |
| --- | --- | --- |
| unit | nothing | mapping derivation, `_id` hashing, bulk item classification, byte-budget splitting (exact NDJSON boundaries, 50×2 MiB), retries (fake client and `httptest`), version selection (default 8, `7`, `8`, `9`, invalid) |
| Integration | ES8 / ES7.17 / ES9.5 | index create / `auto-create=false` failure, mapping compatibility, explicit mapping file, per-type storage read back over HTTP, insert/update/delete, idempotent re-index, composite keys, shared index, bulk partial failure, schema re-check; the ES7 and ES9 suites reuse the same harness through their own adapter |
| E2E | MySQL + ES7.17 / ES8 / ES9.5 | snapshot then binlog, Transform/Route (final table + index + `_id`), checkpoint restart, binlog-only catalog, mapping conflict, ES-unavailable errors, document-level consistency |
| Load | MySQL + ES8 | sustained insert/update/delete + mixed DML, `write-batch-size` sweep, peak writers, bounded catch-up, streaming checksums (`COUNT, SUM(id), SUM(CRC32)`) |
| Scale | MySQL + ES8 | 5 DBs / 20 tables (normal) and 10 DBs / 50 tables + hotspot burst/peak (heavy), per-table checksums, marker and content samples |

Notes:

- Verification always reads documents back (`_search`/scroll/`_mget`/`_count`) and
  compares them with an independent oracle (`sinks/elasticsearch/harness/es_verify.py`); a bulk HTTP
  200 is never treated as proof of content. `_count` is only used as a progress
  gate, and bulk/scaled checks paginate (scroll, 1000 docs/page), never the
  default 10k window.
- CI runs the unit tests plus the `esintegration` suite when the ES service is
  available; E2E, Load, and Scale are local/opt-in because they need MySQL with
  binlog, a long wall clock, and (heavy) a dedicated machine.
- `tpl01_typed.c_bit` is dropped in the Load suite: sink v1 rejects `BIT`
  columns by design.

### Manual smoke test against Elasticsearch 8 + MySQL

```bash
docker run --rm -p 9200:9200 -e discovery.type=single-node \
  -e xpack.security.enabled=false docker.elastic.co/elasticsearch/elasticsearch:8.15.0
```

For Elasticsearch 7, run the same steps against
`docker.elastic.co/elasticsearch/elasticsearch:7.17.24` and add `version: 7` to
`sink`. For Elasticsearch 9, use
`docker.elastic.co/elasticsearch/elasticsearch:9.5.2` and `version: 9`.

1. Point `configs/demo-elasticsearch.yaml` at `http://127.0.0.1:9200`, set
   `auto-create: true`, run `./bin/go-cdc configs/demo-elasticsearch.yaml`, and
   confirm the snapshot documents appear (`GET demo_db.users/_search`).
2. While the job runs, `INSERT`, `UPDATE`, and `DELETE` a row; the same `_id`
   must be overwritten and then removed.
3. Add `mapping-file: ./my-mapping.json` with extra `settings`; the created
   index must keep those settings and the derived fields.
4. Pre-create the index with an incompatible field (for example
   `{"mappings":{"properties":{"id":{"type":"text"}}}}`); startup must fail
   naming the index and the column.
5. Make Elasticsearch reject one document (for example map `id` as `byte` and
   write `1000`); the job must fail and `checkpoint.file-path` must not advance.
6. Use a wrong `username` / `password`; startup must fail with a clear
   authentication error and no credentials in the message.
7. Stop Elasticsearch and start the job; startup must fail on `Ping` with the
   configured hosts (credentials stripped).
8. Set `auto-create: false` and delete the index; startup must fail with
   `does not exist and sink.auto-create is false`.

## Related Docs

- [Pipeline](../pipeline.md)
- [MySQL Source](mysql-source.md)
- [Kafka Sink](kafka-sink.md)
