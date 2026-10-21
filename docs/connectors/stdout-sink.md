# Stdout Sink Connector

Writes each change as one **newline-delimited JSON** object to process stdout. Useful for debugging capture and transforms.

**YAML:** top-level `sink` with `type: stdout`.

## Version & Dependencies

| Item | Support |
| --- | --- |
| Runtime | Any host that can run `go-cdc` |
| External systems | None |
| Packaging | Shipped inside the `go-cdc` binary |

## Example

```yaml
sink:
  type: stdout
```

Full job: [QuickStart](../QuickStart.md) or [`configs/demo-stdout.yaml`](../../configs/demo-stdout.yaml).

## Options

| Option | Required | Default | Type | Description |
| --- | --- | --- | --- | --- |
| `type` | yes | — | String | Must be `stdout` |

No host, user, or password is needed. Other `sink.*` keys used by MySQL sink are not required for stdout.

## Output Format

One JSON object per line. The envelope is **shared with the Kafka sink**
(Debezium-inspired; not a full Debezium `schema`/`payload` wrapper).

| Field | Meaning |
| --- | --- |
| `before` / `after` | Row images (`null` when not applicable) |
| `source.version` | `go-mysql` module version from the build |
| `source.connector` | Always `mysql` |
| `source.name` | Pipeline name (or Kafka `source-name`) |
| `source.ts_ms` | Binlog event time (Unix ms), or `null` if unknown |
| `source.db` / `source.table` | Capturing table |
| `op` | `r` / `c` / `u` / `d` / `ddl` |
| `ts_ms` | Encode time at the sink (Unix ms) |
| `ddl` | Present only for `op=ddl` |

```json
{"before":null,"after":{"id":1,"city":"hz","amt":9.5},"source":{"version":"v1.16.0","connector":"mysql","name":"demo-stdout","ts_ms":null,"db":"demo_db","table":"orders"},"op":"c","ts_ms":1780000000100}
```

## Related Docs

- [QuickStart](../QuickStart.md)
- [Pipeline](../pipeline.md)
- [Kafka Sink](kafka-sink.md) (same JSON envelope)
- Source connectors
  - [MySQL Source](mysql-source.md)
- Sink connectors
  - [MySQL Sink](mysql-sink.md)
