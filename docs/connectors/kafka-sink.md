# Kafka Sink Connector

Writes each change as one **Debezium-inspired JSON** message to Apache Kafka
using the pure-Go client [`franz-go`](https://github.com/twmb/franz-go)
(**no CGo / librdkafka**).

**YAML:** top-level `sink` with `type: kafka`.

## Version & Dependencies

| Item | Support |
| --- | --- |
| Runtime | Any host that can run `go-cdc` |
| Kafka | Protocol negotiation via franz-go; CI verifies against **Apache Kafka 3.8.x** |
| Packaging | Shipped inside the `go-cdc` binary |

## Delivery semantics (at-least-once)

Runner **Flush()**s the sink before persisting a checkpoint. Produce errors
fail the job and **do not** advance the checkpoint. After a crash where Kafka
already accepted messages but the checkpoint file was not saved yet, recovery
may **re-send** those events. Downstream consumers must tolerate duplicates.
This release does **not** provide end-to-end exactly-once or Kafka transactions.

Producer defaults (not YAML knobs unless noted): `acks=all` (all ISR) and no
compression. Flush/Close wait is YAML-controlled via `flush-timeout` (default
`30s`). Producer batch cap is YAML-controlled via `producer-batch-max-bytes`
(default `10485760` / 10MiB; franz-go default is ~1MiB). That client cap must
stay ≤ the broker `message.max.bytes` (IT compose uses 10MiB). It only raises
the batch/record encode ceiling; small messages are unchanged. Envelope size
still grows with base64 / JSON overhead — do not treat 10MiB as “unlimited”.
This is a Producer-layer batch bound, distinct from runner `write-batch-size`
/ `write-batch-interval` (when the job hands a DML batch to the sink). Client
auto topic creation is YAML-controlled via `allow.auto.create.topics`
(default `true`).

## Example

```yaml
sink:
  type: kafka
  brokers: 127.0.0.1:9092
  topic: cdc_events
  source-name: demo-mysql
  schema.change.emit.enabled: false
  # allow.auto.create.topics: true   # default; set false when topics are pre-created
  # producer-batch-max-bytes: 10485760
  # flush-timeout: 30s

route:
  - source-table: demo_db\.users
    sink-table: kafka.users
```

Full job: [`configs/demo-kafka.yaml`](../../configs/demo-kafka.yaml).

## Options

| Option | Required | Default | Type | Description |
| --- | --- | --- | --- | --- |
| `type` | yes | — | String | Must be `kafka` |
| `brokers` | yes | — | String or list | Comma-separated or YAML list of `host:port` |
| `topic` | yes | — | String | Default topic when no route rewrite applies |
| `source-name` | no | `pipeline.name` | String | Envelope `source.name` |
| `client-id` | no | `go-cdc-<pipeline.name>` | String | Kafka client id |
| `schema.change.emit.enabled` | no | `false` | Boolean | When `true`, emit DDL envelopes |
| `allow.auto.create.topics` | no | `true` | Boolean | When `true`, the franz-go producer sends `AllowAutoTopicCreation` on first produce to a missing topic. This is a **client** flag only — the broker must still allow create (`auto.create.topics.enable`, ACLs). Set `false` in production and pre-create topics when auto-create is undesirable |
| `producer-batch-max-bytes` | no | `10485760` (10MiB) | Int (bytes) | franz-go `ProducerBatchMaxBytes` in **512 .. 104857600** (bytes). Producer batch/record encode ceiling, not a single-message size guarantee. Should stay ≤ broker `message.max.bytes`. Distinct from runner `write-batch-*` |
| `flush-timeout` | no | `30s` | Duration | Shared `Flush` / `Close` wait for in-flight produce. Prefer a Go duration string (`30s`, `1m`); a bare integer is milliseconds |
| `write-batch-size` | no | `1000` | Int | Shared runner option: flush DML batch when size reached |
| `write-batch-interval` | no | `1s` | Duration | Shared runner option: flush DML batch on this interval |

`CreateTable` / internal schema callbacks never invent Kafka DDL messages.
Only source DDL events reach Kafka, and only when the emit switch is on.

## Topic routing

1. Events are transformed and **routed** like other sinks (`route.sink-table`
   must be `database.table`).
2. If the routed sink id differs from the source id, the Kafka **topic** is
   that sink id (`database.table`) — **DML and DDL share this rule** for the
   same table.
3. Otherwise the message goes to `sink.topic` (unrouted tables / default).

Envelope `source.db` / `source.table` always name the **capturing** table;
only the topic uses the routed sink id.

Unroutable / invalid topics fail the write (no silent drop).

## Message key

When `Keys` are present, the key is the `fmt.Sprint` of each key column from
`after` (or `before` on delete), joined with `|`. Otherwise the key is empty
(Kafka partitioning is then by broker default).

## Message format

Shared with [stdout](stdout-sink.md). See that page for field definitions
(`before` / `after` / `source` / `op` / `ts_ms` / `ddl`).

> **Note:** stdout previously used top-level `database` / `table` fields. Both
> sinks now use `source.db` / `source.table` only — update existing stdout
> consumers when upgrading.

## Tests

```bash
# unit (FakeProducer; no broker; CI go-check / make test)
go test ./connector/sink/kafka/ ./common/cdcjson/ ./composer/ -count=1

# optional local broker smoke (build tag kafka_integration; NOT in CI)
# requires a reachable broker; default allow.auto.create.topics=true (or pre-create topics)
KAFKA_BROKERS=127.0.0.1:9092 go test -tags kafka_integration ./connector/sink/kafka/ -count=1 -v

# Docker Compose IT (MySQL 8 + Kafka 3.8) — CI: mysql-mysql-e2e (8.0 matrix only)
bash test/integration/scripts/mysql-up.sh
bash test/integration/scripts/kafka-up.sh
bash test/integration/scripts/run-kafka-e2e.sh
# report written at: docs/test/reports/mysql-kafka.md (after a successful run)
```

| Suite | Broker required | CI |
| --- | --- | --- |
| `go test ./connector/sink/kafka/` (no tags) | No | Yes (`make test`) |
| `go test -tags kafka_integration ./connector/sink/kafka/` | Yes (`KAFKA_BROKERS`) | No (local optional) |
| `test/integration` Python TP-K01–K08 | Yes (compose Kafka 3.8) | Yes (MySQL 8.0 matrix) |

## Related Docs

- [Pipeline](../pipeline.md)
- [Stdout Sink](stdout-sink.md) (same JSON envelope)
- [MySQL Source](mysql-source.md)
