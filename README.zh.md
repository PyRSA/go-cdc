# go-cdc

[English](README.md)

## 简介

go-cdc 是一个用 YAML 描述同步作业的 CDC 工具：从 MySQL 捕获全量与增量变更，可选变换与路由后，写出到 **标准输出**、**MySQL**、**Kafka**、**Doris**、**Elasticsearch 7/8/9** 或 **PostgreSQL**（Kafka / Doris 至少一次；Elasticsearch 走 Bulk API 幂等写入；PostgreSQL 为当前状态写入）。一个进程、一份配置、一个二进制：`go-cdc`。

本版本能力：

- MySQL 源（全量快照 + binlog）
- Stdout Sink（换行 JSON）、MySQL Sink、Kafka Sink（至少一次）、Doris Sink（UNIQUE KEY + Stream Load，至少一次）、Elasticsearch 7/8/9 Sink（`sink.version`，默认 8），以及 PostgreSQL Sink（当前状态写入，自动建表与结构变更同步需显式开启）
- 列投影、行过滤、表路由、结构变更策略、文件检查点
- **注意：** stdout JSON 已改为与 Kafka 共用的 Debezium 风格信封（`source.db` / `source.table`），不再使用顶层 `database` / `table`

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
  - [Kafka Sink](docs/connectors/kafka-sink.md)
  - [Doris Sink](docs/connectors/doris-sink.md)
  - [Elasticsearch Sink](docs/connectors/elasticsearch-sink.md)
  - [PostgreSQL Sink](docs/connectors/postgresql-sink.md)

**最小示例（MySQL → stdout）：**

1. 编辑 [`configs/demo-stdout.yaml`](configs/demo-stdout.yaml)（地址、账号、`tables`）。
2. 启动作业：

```bash
./bin/go-cdc configs/demo-stdout.yaml
```

3. MySQL → MySQL 请使用 [`configs/demo-mysql.yaml`](configs/demo-mysql.yaml)。
4. MySQL → Doris 请使用 [`configs/demo-doris.yaml`](configs/demo-doris.yaml)。
5. MySQL → Elasticsearch 请使用 [`configs/demo-elasticsearch.yaml`](configs/demo-elasticsearch.yaml)。

## 性能

go-cdc 在 GitHub Actions 中跑自动化 MySQL 集成与规模测试，覆盖全量快照、
增量 CDC、多库同步、热点表压力，以及持续写入积压下的追平行为。

下表为 **10 次测试平均**（load：每 MySQL 版本 10 次；scale heavy：10 次）。**源端写入**与
**CDC apply** 是不同指标。曲线按追平进度归一化后平均，并压缩 lag 平台期采样。
负载峰值 knobs 现为 **4×60s**；下方追平时长/曲线仍来自此前 **4×90s** 样本，待重采。

### 负载测试（Load）

| 指标 | MySQL 5.7.42 | MySQL 8.0.36 |
| --- | --- | --- |
| **测试类型** | **Load**（func + types + load） | **Load**（func + types + load） |
| 快照 | 约 50 万行 / ~130s（平均） | 约 50 万行 / ~152s（平均） |
| 峰值源端写入 | 60s（估单表 src ≈150 万） | 60s（估单表 src ≈120 万） |
| 受压表 | 2（`tpl01_t1` + typed） | 2（`tpl01_t1` + typed） |
| CDC 合计 apply | 约 2.8K 行/s（两表合计，平均） | 约 1.9K 行/s（两表合计，平均） |
| CDC 追平 | ~2035s（约 34 min）内 lag ≤1%\* | ~2280s（约 38 min）内 lag ≤1%\* |
| 结果 | **PASS** | **PASS** |

\*此前 4×90s CI 平均；4×60s 下墙钟时间粗估约 ⅔。

追平曲线（同一 load 套件，5.7 vs 8.0，平均；此前 4×90s）：   
![Load peak catch-up lag % vs time (MySQL 5.7 vs 8.0)](docs/performance/load-peak-catchup-lag.svg)

### 规模测试（Scale heavy）

仅 MySQL **8.0.36**（CI 无 5.7 scale 矩阵）：10 个源库、50 张表，含并发热点负载；
与 load 分图展示。

| 指标 | 结果（10 次 heavy 平均） |
| --- | --- |
| **测试类型** | **Scale**（heavy） |
| 库 / 表 | 10 库 / 50 表 |
| 快照 | 30 万行 / ~133s |
| 热点 burst | 50 万行 / ~17s |
| 峰值源端写入 | 约 117 万行 / 45s（约 2.6 万行/s） |
| 热点表 | 10 |
| CDC 追平 | ~1302s 内 lag ≤1%（单次 577s / 2027s） |
| 最终校验 | COUNT + SUM(id) + CRC 通过 |
| 结果 | **PASS** |

热点追平曲线（10 表平均 lag，再跨 run 平均）：  
![Scale hotspot catch-up lag % vs time (MySQL 8.0)](docs/performance/scale-hotspot-catchup-lag.svg)

上述结果来自 GitHub Actions 共享 runner，不能当作厂商级 benchmark；实际表现
取决于 CI 环境、MySQL 配置、负载与测试参数。

完整工况、方法与 CI 明细见
[性能压测：MySQL → MySQL（E2E）](docs/performance/mysql-to-mysql-e2e.md)。

## 如何贡献

1. Fork 仓库并创建功能分支。
2. 改动尽量聚焦，遵循现有包结构（`cmd/`、`composer/`、`connector/`、`runtime/`）。
3. 提交 PR 前请本地执行：
   - `make fmt-check`
   - `make lint`
   - `make test`
   - 若改动同步链路，尽量跑集成测试：`make it-mysql && make it-down SINK=mysql`
4. 在 PR 中说明问题与验证方式。

集成测试说明：[tests/integration/README.zh.md](tests/integration/README.zh.md)。

## 许可

本项目使用 [MIT License](LICENSE)。
