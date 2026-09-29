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

One JSON object per line:

| `op` | Meaning | Main fields |
| --- | --- | --- |
| `r` | Snapshot row | `database`, `table`, `after` |
| `c` | Insert | `database`, `table`, `after` |
| `u` | Update | `database`, `table`, `before`, `after` |
| `d` | Delete | `database`, `table`, `before` |
| `ddl` | Schema change | `database`, `table`, `ddl` |

```json
{"op":"c","database":"demo_db","table":"orders","after":{"id":1,"city":"hz","amt":9.5}}
```

## Related Docs

- [QuickStart](../QuickStart.md)
- [Pipeline](../pipeline.md)
- Source connectors
  - [MySQL Source](mysql-source.md)
- Sink connectors
  - [MySQL Sink](mysql-sink.md)
