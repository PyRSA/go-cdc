# go-cdc

[中文](README.zh.md)

## Introduction

go-cdc is a YAML-driven Change Data Capture (CDC) tool. You describe one sync job in a single YAML file: capture full and incremental changes from MySQL, apply optional transform and route, then write to **stdout** or **MySQL**. One process, one config, one binary: `go-cdc`.

Features in this release:

- MySQL source (snapshot + binlog)
- Stdout sink (newline-delimited JSON) and MySQL sink
- Projection, filter, table route, schema-change policy, file checkpoints

## Build Environment

| Item | Requirement |
| --- | --- |
| OS | Linux / macOS / Windows |
| Go | **1.25+** (see `go.mod`) |
| Source / sink DB | MySQL **5.7** or **8.0** |
| Source binlog | `binlog_format=ROW`, `binlog_row_image=FULL` |

### Install with `install.sh` / `install.ps1`

Builds from source, then installs the `go-cdc` binary.

| Platform | Default install path | Script |
| --- | --- | --- |
| Linux / macOS | `$HOME/bin` | [`install.sh`](install.sh) |
| Windows | `%USERPROFILE%\bin` | [`install.ps1`](install.ps1) |

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

See the guides under [`docs/`](docs/):

- [QuickStart](docs/QuickStart.md)
- [Pipeline](docs/pipeline.md)
- Source connectors
  - [MySQL Source](docs/connectors/mysql-source.md)
- Sink connectors
  - [MySQL Sink](docs/connectors/mysql-sink.md)
  - [Stdout Sink](docs/connectors/stdout-sink.md)

**Minimal run (MySQL → stdout):**

1. Edit [`configs/demo-stdout.yaml`](configs/demo-stdout.yaml) (host, user, password, `tables`).
2. Start the job:

```bash
./bin/go-cdc configs/demo-stdout.yaml
```

3. For MySQL → MySQL, use [`configs/demo-mysql.yaml`](configs/demo-mysql.yaml).

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
