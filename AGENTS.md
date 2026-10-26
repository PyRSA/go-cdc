# go-cdc

YAML-driven MySQL CDC. Binary: `go-cdc`.

## READMEs

- `README.md` (EN) · `README.zh.md` (ZH)

## User documentation (English)

- `docs/QuickStart.md`
- `docs/pipeline.md`
- `docs/connectors/mysql-source.md`
- `docs/connectors/mysql-sink.md`
- `docs/connectors/stdout-sink.md` — stdout + shared CDC JSON
- `docs/connectors/kafka-sink.md` — Kafka (franz-go, at-least-once)
- `docs/connectors/doris-sink.md` — Doris 2.1.x (UNIQUE KEY, Stream Load, at-least-once)
- `docs/connectors/elasticsearch-sink.md` — Elasticsearch 8 (go-elasticsearch/v8)
- `docs/performance/mysql-to-mysql-e2e.md` — MySQL→MySQL CI performance detail (README summary only)

Document only options accepted by `composer/definition/parse.go`.

## Commands

```bash
make build
make fmt-check
make lint
make test
```
