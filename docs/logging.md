# Logging

go-cdc uses Go `log/slog` with the existing text handler in `common/logfmt`. An optional top-level `logging:` block configures level, format, and outputs. When the block is omitted, behaviour matches previous releases: **INFO**, **text**, **stderr**.

## Options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `logging.level` | no | `INFO` | `DEBUG`, `INFO`, `WARN`, or `ERROR` |
| `logging.format` | no | `text` | `text` (logfmt handler) or `json` (`slog.JSONHandler`) |
| `logging.console` | no | — | Present → enable console output |
| `logging.console.stream` | no | `stderr` | `stdout` or `stderr` |
| `logging.file` | no | — | Present → enable file output |
| `logging.file.path` | yes (if file) | — | Log file path; parent directories are created |
| `logging.file.rotation` | no | `100 MB` | Size or time trigger (see below) |
| `logging.file.retention` | no | keep all | Age duration or file count |
| `logging.file.compression` | no | none | `gz` / `gzip`, or empty |

There is no `enabled` flag: configure `console` and/or `file` to turn them on. Both may be set; each record is written to every configured target. If `logging:` is present but neither target is set, output stays on stderr (same as the historical default).

## Rotation

Exactly one style per value:

| Form | Example | Behaviour |
| --- | --- | --- |
| Size | `100 MB`, `50MB`, `1 GiB`, `512 KiB` | Rotate when the active file would exceed the size |
| Duration | `1 day`, `12 hours`, `30m`, `24h` | Rotate when the interval elapses since the file was opened or last rotated |
| Daily clock | `00:00`, `9:30` | Rotate at that local time each calendar day |

**Duration rotation** is an interval timer from process open / last rotate, not from the file's mtime and not a wall-clock schedule. Restarting the process resets the timer. Prefer `HH:MM` when you need a fixed local time of day. A due time rotate is skipped when the active file is still empty; the next deadline advances so empty archives are not created.

A single log record larger than the size limit is still written in full into a fresh file; that file is then archived so the next record starts another file. Archive names include a monotonic sequence. If the chosen name is already present (including after a process restart), rotation claims a new name instead of replacing the existing file.

Rotation builds each archive in a temporary file first, then publishes it to the final name only when the copy is complete. Incomplete temps are not treated as archives. The same pattern applies when compressing to `.gz`.

Supported size units: `B`/`byte(s)`, `KB`/`KiB`, `MB`/`MiB`, `GB`/`GiB` (case-insensitive). Bare `m`/`k`/`g` are not size units so Go durations like `30m` stay minutes. Unsupported expressions fail startup with `logging.file.rotation: …`.

## Retention

| Form | Example | Behaviour |
| --- | --- | --- |
| Age | `30 days`, `24h`, `2 weeks` | Delete archives older than the duration |
| Count | `10`, `10 files` | Keep the newest N **logical** archives |

A plain archive and its `.gz` companion count as one logical archive, so a failed cleanup after compression cannot consume two retention slots.

Omit `retention` to keep archives indefinitely (until the operator removes them).

## Compression

| Value | Behaviour |
| --- | --- |
| omit / empty / `none` | Archives stay plain files |
| `gz` or `gzip` | Each rotated file is gzip-compressed (`*.gz`) |

If compression fails, the plain archive is kept and no final `.gz` is published, so a later retry can succeed. If compression succeeds but deleting the plain file fails, both files may remain temporarily; retention still treats them as one archive. Existing `.gz` files are never overwritten.

Other values (for example `zip`) are rejected.

## Examples

Console only:

```yaml
logging:
  level: INFO
  format: text
  console:
    stream: stderr
```

File only:

```yaml
logging:
  level: INFO
  format: text
  file:
    path: ./logs/go-cdc.log
    rotation: 100 MB
    retention: 30 days
    compression: gz
```

Console and file:

```yaml
logging:
  level: DEBUG
  format: json
  console:
    stream: stdout
  file:
    path: ./logs/go-cdc.log
    rotation: 1 day
    retention: 10
```

## Notes

- Text format keeps the existing `[LEVEL] [module] message key=value` layout from `common/logfmt`.
- JSON format uses the standard library `slog.JSONHandler` attribute names (`msg`, `level`, `time`, …).
- File rotation, retention, and gzip use a small stdlib rolling writer (`common/logging`); they do not change CDC business behaviour.
