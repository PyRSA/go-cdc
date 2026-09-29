# go-cdc

[English](README.md)

## 简介

go-cdc 是一个用 YAML 描述同步作业的 CDC 工具：从 MySQL 捕获全量与增量变更，可选变换与路由后，写出到 **标准输出** 或 **MySQL**。一个进程、一份配置、一个二进制：`go-cdc`。

本版本能力：

- MySQL 源（全量快照 + binlog）
- Stdout Sink（换行 JSON）与 MySQL Sink
- 列投影、行过滤、表路由、结构变更策略、文件检查点

## 环境构建

| 项 | 要求 |
| --- | --- |
| 操作系统 | Linux / macOS / Windows |
| Go | **1.25+**（见 `go.mod`） |
| 源 / 目标库 | MySQL **5.7** 或 **8.0** |
| 源库 binlog | `binlog_format=ROW`，`binlog_row_image=FULL` |

### 使用 `install.sh` / `install.ps1` 安装

从源码编译并安装 `go-cdc` 二进制。

| 平台 | 默认安装路径 | 脚本 |
| --- | --- | --- |
| Linux / macOS | `$HOME/bin` | [`install.sh`](install.sh) |
| Windows | `%USERPROFILE%\bin` | [`install.ps1`](install.ps1) |

**Linux / macOS**

```bash
# 用户目录 → $HOME/bin
curl -sSfL https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.sh | bash -s

# 安装到 $(go env GOPATH)/bin
curl -sSfL https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.sh | bash -s -- -b "$(go env GOPATH)/bin"

# 安装到当前目录 ./bin
curl -sSfL https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.sh | bash -s -- -b ./bin

# 全局 → /usr/local/bin（可能需要 sudo）
curl -sSfL https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.sh | bash -s -- --global
```

本地仓库：

```bash
git clone https://github.com/PyRSA/go-cdc.git
cd go-cdc

./install.sh                  # $HOME/bin
./install.sh --user           # 同默认
./install.sh --global         # /usr/local/bin
./install.sh -b /opt/go-cdc/bin
make build                    # 仅编译 → ./bin/go-cdc
```

**Windows（PowerShell）**

```powershell
# 一键安装（编译并装到 %USERPROFILE%\bin，并更新用户 PATH）
irm https://raw.githubusercontent.com/PyRSA/go-cdc/main/install.ps1 | iex

# 或在本地仓库执行
git clone https://github.com/PyRSA/go-cdc.git
cd go-cdc

.\install.ps1
.\install.ps1 -BinDir "$env:USERPROFILE\bin"
.\install.ps1 -BinDir "$(go env GOPATH)\bin"
```

可选检查：

```bash
make fmt-check   # gofmt
make lint        # golangci-lint（.golangci.yml）
make test        # go test -race
```

## 快速使用

文档见 [`docs/`](docs/)：

- [QuickStart](docs/QuickStart.md)
- [Pipeline](docs/pipeline.md)
- Source connectors
  - [MySQL Source](docs/connectors/mysql-source.md)
- Sink connectors
  - [MySQL Sink](docs/connectors/mysql-sink.md)
  - [Stdout Sink](docs/connectors/stdout-sink.md)

**最小示例（MySQL → stdout）：**

1. 编辑 [`configs/demo-stdout.yaml`](configs/demo-stdout.yaml)（地址、账号、`tables`）。
2. 启动作业：

```bash
./bin/go-cdc configs/demo-stdout.yaml
```

3. MySQL → MySQL 请使用 [`configs/demo-mysql.yaml`](configs/demo-mysql.yaml)。

## 如何贡献

1. Fork 仓库并创建功能分支。
2. 改动尽量聚焦，遵循现有包结构（`cmd/`、`composer/`、`connector/`、`runtime/`）。
3. 提交 PR 前请本地执行：
   - `make fmt-check`
   - `make lint`
   - `make test`
   - 若改动同步链路，尽量跑集成测试：`make integration-up && make integration-test && make integration-down`
4. 在 PR 中说明问题与验证方式。

集成测试说明：[test/integration/README.zh.md](test/integration/README.zh.md)。

## 许可

本项目使用 [MIT License](LICENSE)。
