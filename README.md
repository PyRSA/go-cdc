# go-cdc

[中文](README.zh.md)

## Introduction

go-cdc is a YAML-driven Change Data Capture (CDC) tool. You describe one sync job in a single YAML file: capture full and incremental changes from MySQL, apply optional transform and route, then write to **stdout** or **MySQL**. One process, one config, one binary: `go-cdc`.

Features in this release:

- MySQL source (snapshot + binlog)
- Stdout sink (newline-delimited JSON) and MySQL sink
- Projection, filter, table route, schema-change policy, file checkpoints

## Build Environment


| Item             | Requirement                                  |
| ---------------- | -------------------------------------------- |
| OS               | Linux / macOS / Windows                      |
| Go               | **1.25+** (see `go.mod`)                     |
| Source / sink DB | MySQL **5.7** or **8.0**                     |
| Source binlog    | `binlog_format=ROW`, `binlog_row_image=FULL` |




### Install with `install.sh` / `install.ps1`

Builds from source, then installs the `go-cdc` binary.


| Platform      | Default install path | Script                       |
| ------------- | -------------------- | ---------------------------- |
| Linux / macOS | `$HOME/bin`          | `[install.sh](install.sh)`   |
| Windows       | `%USERPROFILE%\bin`  | `[install.ps1](install.ps1)` |


**Linux / macOS**

```bash
# User install → $HOME/bin
curl -sSfL https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.sh | bash -s

# Install into $(go env GOPATH)/bin
curl -sSfL https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.sh | bash -s -- -b "$(go env GOPATH)/bin"

# Install into ./bin (current directory)
curl -sSfL https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.sh | bash -s -- -b ./bin

# Global → /usr/local/bin (may need sudo)
curl -sSfL https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.sh | bash -s -- --global
```

From a local clone:

```bash
git clone https://github.com/PyRSA/go-cdc.git
cd go-cdc

./install.sh                  # $HOME/bin
./install.sh --user           # same as default
./install.sh --global         # /usr/local/bin
./install.sh -b /opt/go-cdc/bin
make build                    # build only → ./bin/go-cdc
```

**Windows (PowerShell)**

```powershell
# One-liner (build + install to %USERPROFILE%\bin, update user PATH)
irm https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.ps1 | iex

# Or from a local clone
git clone https://github.com/PyRSA/go-cdc.git
cd go-cdc

.\install.ps1
.\install.ps1 -BinDir "$env:USERPROFILE\bin"
.\install.ps1 -BinDir "$(go env GOPATH)\bin"
```

Optional checks:

```bash
make fmt-check   # gofmt
make lint        # golangci-lint (.golangci.yml)
make test        # go test -race
```



## Quick Start

See the guides under `[docs/](docs/)`:

- [QuickStart](docs/QuickStart.md)
- [Pipeline](docs/pipeline.md)
- Source connectors
  - [MySQL Source](docs/connectors/mysql-source.md)
- Sink connectors
  - [MySQL Sink](docs/connectors/mysql-sink.md)
  - [Stdout Sink](docs/connectors/stdout-sink.md)

**Minimal run (MySQL → stdout):**

1. Edit `[configs/demo-stdout.yaml](configs/demo-stdout.yaml)` (host, user, password, `tables`).
2. Start the job:

```bash
./bin/go-cdc configs/demo-stdout.yaml
```

1. For MySQL → MySQL, use `[configs/demo-mysql.yaml](configs/demo-mysql.yaml)`.



## Performance

go-cdc includes automated MySQL integration and scale tests in GitHub Actions
to validate snapshot replication, incremental CDC processing, multi-database
replication, hotspot workloads, and catch-up behavior under sustained write
pressure.

Results below are from the same `master` CI day (2026-09-30). **Source write**
and **CDC apply** are different metrics.

### Load test


| Metric              | MySQL 5.7.42                   | MySQL 8.0.36                   |
| ------------------- | ------------------------------ | ------------------------------ |
| **Test type**       | **Load** (func + types + load) | **Load** (func + types + load) |
| Snapshot            | ~500K rows / 168.5s            | ~500K rows / 228.7s            |
| Peak source write   | 3.05M rows / 90s (~34K rows/s) | 2.24M rows / 90s (~25K rows/s) |
| Stressed tables     | 2 (`tpl01_t1` + typed)         | 2 (`tpl01_t1` + typed)         |
| Aggregate CDC apply | ~2.9K rows/s (2 tables)        | ~1.5K rows/s (2 tables)        |
| CDC catch-up        | ≤1% lag in 2023s (~34 min)     | ≤1% lag in 2983.5s (~50 min)   |
| Result              | **PASS**                       | **PASS**                       |


Lag-over-time chart (5.7 vs 8.0 catch-up):  
![Load peak catch-up lag % vs time (MySQL 5.7 vs 8.0)](docs/performance/load-peak-catchup-lag.svg)。

### Scale test (heavy)

MySQL **8.0.36** only (no 5.7 scale matrix in CI): 10 source databases, 50
tables, concurrent hotspot workloads.


| Metric              | Result                                |
| ------------------- | ------------------------------------- |
| **Test type**       | **Scale** (heavy)                     |
| Databases / tables  | 10 DBs / 50 tables                    |
| Snapshot            | 300K rows / 104.1s                    |
| Hotspot burst       | 500K rows / 15.7s                     |
| Peak source write   | 1.356M rows / 45s (~30K rows/s)       |
| Hotspot tables      | 10                                    |
| Aggregate CDC apply | ~3.1K rows/s across 10 hotspot tables |
| CDC catch-up        | ≤1% lag in 577s                       |
| Final verification  | COUNT + SUM(id) + CRC passed          |
| Result              | **PASS**                              |


These results come from GitHub Actions shared runners and should not be
treated as vendor-grade benchmarks. Results depend on the CI environment,
MySQL configuration, workload, and test parameters.

For detailed workload definitions, test methodology, and CI results, see
[Performance Stress Testing: MySQL → MySQL (E2E)](docs/performance/mysql-to-mysql-e2e.md).

## Contributing

1. Fork the repository and create a feature branch.
2. Keep changes focused; follow existing package layout (`cmd/`, `composer/`, `connector/`, `runtime/`).
3. Before opening a PR:
  - `make fmt-check`
  - `make lint`
  - `make test`
  - For behavior that touches sync paths, run integration tests when possible: `make integration-up && make integration-test && make integration-down`
4. Describe the problem and how you verified the fix in the PR description.

Integration harness notes: [test/integration/README.md](test/integration/README.md).

## License

This project is licensed under the [MIT License](LICENSE).
