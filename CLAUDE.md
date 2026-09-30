# go-cdc

YAML-driven MySQL CDC. Binary: `go-cdc`.

## READMEs

- `README.md` (English) · `README.zh.md` (中文)

## User docs (English under docs/)

| Doc | Content |
| --- | --- |
| `docs/QuickStart.md` | Requirements, stdout quickstart, how to launch |
| `docs/pipeline.md` | Source / Transform / Projection / Route / Schema Evolution / Sink |
| `docs/connectors/mysql-source.md` | MySQL source options |
| `docs/connectors/mysql-sink.md` | MySQL sink options |
| `docs/connectors/stdout-sink.md` | Stdout sink (`type` only) + JSON format |
| `docs/performance/mysql-to-mysql-e2e.md` | MySQL→MySQL CI scale/load performance detail; README has summary |

Demos: `configs/demo-stdout.yaml`, `configs/demo-mysql.yaml`  
Internal design: `docs/architecture.md`

## Commands

```bash
make build
make fmt-check   # gofmt
make lint        # golangci-lint
make test        # go test -race
./bin/go-cdc configs/demo-stdout.yaml
```
