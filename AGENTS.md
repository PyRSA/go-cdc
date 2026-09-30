# go-cdc

YAML-driven MySQL CDC. Binary: `go-cdc`.

## READMEs

- `README.md` (EN) · `README.zh.md` (ZH)

## User documentation (English)

- `docs/QuickStart.md`
- `docs/pipeline.md`
- `docs/connectors/mysql-source.md`
- `docs/connectors/mysql-sink.md`
- `docs/connectors/stdout-sink.md` — only `type: stdout`
- `docs/performance/mysql-to-mysql-e2e.md` — MySQL→MySQL CI performance detail (README summary only)

Document only options accepted by `composer/definition/parse.go`.

## Commands

```bash
make build
make fmt-check
make lint
make test
```
