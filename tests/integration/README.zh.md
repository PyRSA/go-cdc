# go-cdc 集成测试（Docker Compose）

本地与 GitHub Actions 走同一条路径：用 **Docker Compose** 拉起带 binlog 的 MySQL，再针对它跑一个或多个套件。
每个 sink 只有一个入口：

```bash
bash tests/integration/scripts/run.sh <sink> [profile] [variant] [suites] [test-point...]
```

| Sink | 套件（所有 sink 两档都跑全量清单；`heavy` 仅放大峰值写并发与超时） |
| --- | --- |
| `mysql` | `func` TP-01..TP-13（`sinks/mysql/harness/run_it.py`）+ `types` TP-D01..D04 + `load` TP-L01..L04 + `scale` —— 四项两档全跑，5.7 与 8.0 一致 |
| `stdout` | `func` — 共用的 CDC JSON 信封（`sinks/stdout/harness/stdout_it.py`） |
| `kafka` | `func` TP-K01..TP-K08（`sinks/kafka/harness/kafka_it.py`）；负载/峰值测试点就在套件内部 |
| `doris` | `go` sink 套件 + `func` TP-DR01..DR07 + `scale` —— 三项两档全跑 |
| `elasticsearch` | `go` sink 套件 + `func` + `load` + `scale` —— es8 两档都跑全部四项；es7/es9 仍为 `go`、`func`（由变体维度决定，不是档位裁剪） |
| `postgresql` | `go` sink 套件 + `func` TP-P01..P20 + `load`、`bench`、`scale`、`compat` —— 六项两档全跑；`compat` 只验证本矩阵行自己那个 PG 大版本，故 heavy 靠把变体矩阵扩成 5 个并行行来覆盖 pg14..pg18，normal 则固定 pg16 |

`profile` 决定**跑多少**，`variant` 决定**跑在哪个版本上**（`57`/`80`、`pg14`..`pg18`、`es7`/`es8`/`es9`、
Doris 的 `2.1.x` tag）。两者都不从环境推断。映射表只有一处：`logger.sh: it_apply_variant`；
它被调用两次，因为子进程的 export 无法回传给父进程 —— `run.sh` 在自己进程里调它（套件要读
`ES_URL`、`ES_SINK_VERSION`、`PG_EXPECT_MAJOR`），`setup.sh` 在子进程里调它来选镜像
（`MYSQL_IMAGE`、`PG_IMAGE`、`DORIS_VERSION`）。

生成的 Markdown 报告（英文）落在 `docs/test/reports/`：

| 套件 | 报告 |
|------|------|
| 功能 | `docs/test/reports/mysql-mysql-func.md` |
| 类型 | `docs/test/reports/mysql-mysql-types.md` |
| 负载 | `docs/test/reports/mysql-mysql-load.md` |
| 规模 | `docs/test/reports/mysql-mysql-scale-{normal\|heavy}.md` |
| Kafka | `docs/test/reports/mysql-kafka.md` |
| Doris | `docs/test/reports/mysql-doris.md`（含功能 + 负载阶段；Go 套件日志见 `workdir/doris-integration.log`） |
| Doris 规模 | `docs/test/reports/mysql-doris-scale-{normal\|heavy}.md` |

静态说明：[`docs/test/TYPES.zh.md`](../../docs/test/TYPES.zh.md)（不是跑测报告）。

English: [README.md](./README.md)

## 命名约定（匿名账号 + 按测试点）

| 类别 | 命名 | 示例 |
|------|------|------|
| 源/目标账号 | 匿名固定 token | `u_a7k9m2x4` / `u_b3j6c8y1` |
| 库名 | `db_tp_{suite}_{src\|snk}` | `db_tp_func_src`、`db_tp_load_snk` |
| 功能表 | `tpNN_*` | `tp01_rows`、`tp07_fail`、`tp10_add` |
| 类型表 | `tpdNN_*` | `tpd01_types` |
| 负载表 | `tplNN_*` | `tpl01_t1`、`tpl01_typed`、`tpl01_large` |
| 目标表 | 源表名 + `_out` | `tp01_rows_out` |

默认凭据见 `sources/mysql/init/01-databases.sql`（可用环境变量覆盖）。

## 目录结构

```text
tests/integration/
├── conftest.py              # 把本目录放进 sys.path（单一 pytest 树）
├── sources/mysql/           # CDC 源端：compose 文件、init SQL、conf.d
├── sinks/<sink>/
│   ├── docker-compose.yml
│   ├── fixtures/            # pipeline 模板和/或 seed SQL（按需提供；ES/PG 的造数逻辑内联在 Python 代码中）
│   ├── harness/             # *_it.py 入口及其 helper
│   ├── scale/               # 仅当该 sink 有规模层
│   ├── tests/               # 该 harness 的 pytest 单测
│   └── go/                  # 仅 doris：sink 级 Go 套件
├── common/                  # config、日志、集群 profile、共享规模套件
├── scripts/                 # setup.sh · wait.sh · teardown.sh · run.sh · logger.sh
└── workdir/                 # 二进制、日志、checkpoint（gitignored）
```

compose 文件是**叠加**的（`-f sources/mysql/… -f sinks/<sink>/…`），每个 sink 一个 project
（`gocdc-<sink>-mysql`，名字即内容：源端永远在同一个 project 里），
因此 `setup.sh <sink>` 只起“源端 + 被测 sink”，不会顺带拉起别的服务。

## 快速跑

```bash
bash tests/integration/scripts/run.sh mysql             # 全量套件，MySQL 8.0
bash tests/integration/scripts/run.sh mysql heavy       # 同一批套件，放大并发与超时
bash tests/integration/scripts/run.sh mysql normal 57   # 同一批套件跑 5.7
bash tests/integration/scripts/run.sh postgresql normal pg14
ls docs/test/reports/
```

`setup.sh` 既起栈，也**阻塞等到栈可用** —— 探针都在 `wait.sh`：`setup.sh` source 它，
你也可以对一个已经起好的栈单独跑 `bash scripts/wait.sh <sink>`。随后 `run.sh` 编译 `go-cdc`、造数、
按顺序跑套件，最后回收栈 —— 但**仅限本地**。CI 里这三件事是三个独立步骤
（`Setup stack` → `Run suites`（带 `IT_SKIP_SETUP=1`）→ `Teardown`），起栈失败只会记在起栈那一步，
回收是最后一个 `if: always()` 步骤、排在报告上传之后，产物因此不会和 `docker compose down` 抢时序。
逃生口：`IT_KEEP=1` 保留容器、`IT_REUSE=1` 复用数据卷、`IT_SKIP_SETUP=1` 完全跳过起栈。

## 规模套件

`scale` 现在两档都跑，属于每个有规模层的 sink —— 档位只差写并发与超时。要单独跑就用第 4 个位置参数（或等价的 `IT_SUITES`）：

```bash
bash tests/integration/scripts/run.sh mysql heavy            # func,types,load,scale
bash tests/integration/scripts/run.sh mysql normal "" scale  # 只跑规模套件
IT_SUITES=scale bash tests/integration/scripts/run.sh mysql normal
```

规模库（`db_tp_scale_src_NN` / `db_tp_scale_snk_NN`）由 `scale_it.py` 在运行时以 compose root
（`ROOT_USER` / `ROOT_PASSWORD`）创建，再授权给与其他套件相同的匿名源/目标账号。

### Doris 变体

同一套 profile、同样的表/行分配，目标换成 Doris：

```bash
bash tests/integration/scripts/run.sh doris            # go + func + scale
bash tests/integration/scripts/run.sh doris heavy      # 同一批套件，放大并发与超时
make it-doris PROFILE=heavy
```

它**故意不与** MySQL→MySQL 规模套件共用库名（改用 `db_tp_dscale_src_NN` / `db_tp_dscale_snk_NN`），
两者可并行而不会互删对方表。三处差异由 Doris v1 语义决定，报告会逐条写明：

| 差异 | 原因 |
| --- | --- |
| `delete` 事件组的表被重映射为 `insert_update` | Doris sink 拒绝 DELETE，`delete.enabled: true` 在解析期即报错 |
| `wide` 去掉 `c_time TIME`；`large` 用 `payload LONGTEXT` 取代 `binary_data BLOB` | `types.go` 在 v1 中没有 TIME / 二进制类型的映射 |
| 目标库由 `auto-create-table` 建立 | sink 自己执行 `CREATE DATABASE IF NOT EXISTS` |

内容校验在启动时探测：MySQL 与 Doris 对同一固定字符串的 `CRC32` 必须相同，否则降级为字符长度求和
（MySQL 用 `CHAR_LENGTH`，Doris 用 `LENGTH`），并在报告顶部标注已降级，而不是静默比较两种不同的聚合值。

## 负载 knobs（tier A）

| 变量 | 默认 | 说明 |
|------|------|------|
| `LOAD_ROWS_PER_TABLE` | `100000`（两档相同） | 普通表行数（t1–t4 + typed） |
| `LOAD_LARGE_ROWS` | `50` | 大字段表 `tpl01_large` 行数 |
| `LOAD_LARGE_FIELD_BYTES` | `2097152`（2MiB，限制在 1–5MiB） | 单字段大小：图片 `LONGBLOB` + `LONGTEXT` |
| `LOAD_PEAK_SECONDS` | `60`（两档相同） | 峰值写入时长 |
| `LOAD_PEAK_WORKERS` | 跟随档位：`4` normal / `8` heavy | 峰值写并发；**造数**用 `workers × 2`（上限 16，且不超过表数） |
| `LOAD_PARALLELISM` | 默认同 `LOAD_PEAK_WORKERS` | 负载套件 CDC `pipeline.parallelism` |
| `LOAD_PEAK_BATCH` | `200` | 每批 INSERT 行数 |
| `LOAD_PROFILE` | 跟随档位（`normal` / `heavy`） | 分档标签（写入 load 报告） |

规模侧另有双档超时：`PEAK_CATCHUP_BUDGET_S`（`1800` / `5400`）与 `SCALE_TOTAL_TIMEOUT`（`2400` / `7200`）。

负载用例：TP-L01 多表全量（t1–t4 + typed + large）、TP-L02 峰值（t1 + typed）、TP-L03 类型抽检、TP-L04 大字段 LENGTH 校验。

## GitHub Actions

[`ci.yml`](../../.github/workflows/ci.yml) 是唯一入口：

```text
decide  ->  plan  ->  go-check  ->  ci-<sink>.yml（每个受影响的 sink 一次）  ->  result
```

| Job | 做什么 |
| --- | --- |
| `decide` | 丢掉已被 open PR 覆盖的 push，并且是**唯一**计算档位的地方（dispatch 输入、PR 上的 `ci-heavy`、默认分支 push） |
| `plan` | `tools/ci/plan.py`：git diff → `{"include": [{"sink": …, "variant": …}]}`（纯文档改动输出 `{"include": []}`） |
| `go-check` | `gofmt`、`golangci-lint`、`make test`、build，随后 `pytest tests/integration tools/ci/test_plan.py` 与全量脚本 `bash -n` |
| `sink-<sink>` | 可复用的 `ci-<sink>.yml`，只喂给该 sink 自己的矩阵行 |
| `result` | 唯一必需检查；`skipped` 与空矩阵都算成功 |

档位：

| 档位 | 触发 | 跑什么 |
| --- | --- | --- |
| `normal` | PR opened / synchronized / reopened，以及推送到其他任一分支 | 每个受支持 variant 的全量套件清单（MySQL 5.7 + 8.0 `func,types,load,scale`；PG 16 `go,func,load,bench,scale,compat`；Doris `go,func,scale`；ES 8 `go,func,load,scale`；ES 7/9 `go,func`；Kafka、stdout `func`） |
| `heavy` | PR 上已打 `ci-heavy`（打完再 push）或 `gh workflow run ci.yml -f profile=heavy`；push 到 `master`/`main` | 与 normal 完全相同的套件清单；峰值写并发（4 → 8）与规模套件的二级超时（追平预算、总超时）随档位放大，且 PostgreSQL 变体矩阵从 pg16 扩到 pg14..pg18，让 compat 的五个大版本作为 5 个并行行跑 |

每一次 push 与每一个 PR 都是触发条件，`push` 不限定分支。当某次 push 已被一个 open PR 覆盖时，
`decide` 会置 `run=false`，该次 push 不执行任何作业（`result` 仍报成功）—— 这个 commit 由 PR 的那次
run 负责测试。PR 查询失败时按 fail-open 处理：照常跑，并输出 `::warning::`。

`labeled` 故意不作为触发条件：标签事件与正在运行的 run 共享同一个 concurrency group，打**任何**
标签都会取消进行中的 run，并用一个"全部 skipped"的替代它 —— 最终报成 success。正确做法是打上
`ci-heavy` 后再 push，每次 run 都会重新读标签。

仓库卫生：[`repo-checks.yml`](../../.github/workflows/repo-checks.yml) → `CI / Repo Checks`，
每次 push/PR 检查单文件大小与 license 头。

跑哪些 sink 由 diff 决定，而不是 path filter：改 `tests/integration/sinks/kafka/`、`connector/sink/kafka/`、
`configs/demo-kafka.yaml` 或 `.github/workflows/ci-kafka.yml` 只跑 Kafka；改 CDC 核心、IT 管线、`ci.yml`
或构建文件跑全部；纯文档改动什么都不跑（`result` 仍然通过）。`release.yml` 与 `repo-checks.yml` 是独立
工作流，永远不会把矩阵带进来。

```bash
python3 tools/ci/plan.py --paths connector/sink/kafka/sink.go   # {"include":[...]}
make it-plan PATHS="runtime/checkpoint/store.go"                 # 同一个，走 make
```

报告按 `reports-<sink>-<variant>-<profile>` 上传（`docs/test/reports/**`），在每个 `heavy` 行与任何失败时上传；
无 variant 的 sink 用 `default` 占位（如 `reports-kafka-default-heavy`）。

## Kafka sink 套件

```bash
bash tests/integration/scripts/run.sh kafka             # func（快照 + 负载 + 峰值）
bash tests/integration/scripts/run.sh kafka heavy       # 同一套件，放大旋钮
# 报告：docs/test/reports/mysql-kafka.md
```

| TP | 覆盖 |
| --- | --- |
| K01 | snapshot：route + 默认 topic + projection + `city='hz'` 过滤 + `before=null` |
| K02 | 增量 `c`/`u`/`d` + message key；`city=sh` 的插入被过滤后不存在 |
| K03 | DDL emit 关（无 `ddl`）与开（`ddl` 进默认 topic） |
| K04 | 保留 checkpoint 重启；新行与历史行仍可读（ALO 重复可接受） |
| K05 | 负载快照量（`tpl01_t1` / typed / large）以 Kafka end offset ≥99% 源端为准 |
| K06 | 峰值追平（与 mysql→mysql 负载同一批 `LOAD_PEAK_*` 旋钮） |
| K07 | 类型列 JSON 抽检 |
| K08 | 大字段 LONGBLOB/LONGTEXT（JPEG magic + 长度） |

CI 在 `sink-kafka` job 里跑，`LOAD_ROWS_PER_TABLE` / 峰值旋钮与 mysql→mysql 负载套件一致。

## Doris sink 套件

需要 Linux、Docker Compose、host 网络、mysql 客户端，以及 FE/BE 的内核参数：

```bash
sudo sysctl -w fs.inotify.max_user_watches=1048576
sudo sysctl -w fs.inotify.max_user_instances=8192
sudo sysctl -w vm.max_map_count=2000000
```

参数偏低时 `setup.sh doris` 拒绝启动（CI 上无免密 sudo 会自动调高）。未设置 `DORIS_HOST` 时
Go sink 套件自动跳过，普通 `go test ./...` 不会启动 Doris。

```bash
bash tests/integration/scripts/run.sh doris            # go + func
bash tests/integration/scripts/run.sh doris heavy      # + scale
# 报告：docs/test/reports/mysql-doris.md
# Go sink 日志：tests/integration/workdir/doris-integration.log
```

| TP | 覆盖 |
| --- | --- |
| DR01 | snapshot：route + projection + `city='hz'` filter → Doris UNIQUE KEY |
| DR02 | 增量 insert + update；`city=sh` 被过滤（v1 不支持 DELETE） |
| DR03 | `tpds01_evolve`（无 projection）：源端 `ADD COLUMN` → Doris DESC + 新列行 |
| DR04 | 保留 checkpoint 重启；重启后 upsert 收敛（ALO） |
| DR05 | 多表批量快照：`tpdl01_t1`–`t4` + `typed`，每表 `LOAD_ROWS_PER_TABLE` 行 |
| DR06 | 狂写追平：`LOAD_PEAK_*` 并发 INSERT 风暴；Doris 行数收敛到 1% 以内 |
| DR07 | 批量负载下的类型抽检（int/bigint/decimal/varchar/date/datetime/json/bool） |

负载阶段（DR05–DR07）复用与 mysql→mysql / kafka 负载套件相同的 `LOAD_*` 旋钮；两档跑同一批套件，只有并发写与超时随档位放大。
Doris v1 拒绝 BLOB/ENUM/BIT/SET/TIME，因此批量表放在 `db_tp_load_src.tpdl01_*`，只使用可映射类型；
`tpl01_large` 的大字段用例没有 Doris 对应项。

同一次运行的 `go` 套件随后跑 sink 级 Go 测试（`doris_integration`）：自动建表 / UNIQUE KEY、
同批重复主键、空 Schema 缓存重启、额外列、类型拒绝/往返、Label + `SHOW LOAD`（重复 Label 不得追加
第二行、过滤行为、已完成 Label 的重试；若该构建根本不记录 Stream Load 作业，只有"每个 Label 恰好一行"
这个子测试报 `SKIP`，该探针其余断言照常执行）、`DESC` 可空性与空串默认值解析、`ADD COLUMN` 可见性
与"部分列已添加"状态、重启重新校验、多表首败即停。`Publish Timeout` / `CANCELLED` / 5xx 注入仍由
单元 / `httptest` 覆盖；"DDL 已生效但返回错误"也属于该范围。

`sink-doris` job（[`ci-doris.yml`](../../.github/workflows/ci-doris.yml)）启动 MySQL + Doris `2.1.8`
FE+BE、调高内核参数、执行套件（TP-DR01..DR04、sink Go 套件、Doris 规模 —— 两档跑同一批套件），
并上传 Markdown 报告与 `cdc-doris*.log`。

## Elasticsearch 8 sink 套件

ES8 sink 与 MySQL sink 对齐的四层测试。共用 MySQL compose、造数器（`sinks/mysql/harness/load_it.py`、
`common/scale_it.py`）与同名 knob，差异只在目标端：`sinks/elasticsearch/harness/es_client.py`（HTTP）、
`sinks/elasticsearch/harness/es_verify.py`（独立 oracle）。

```bash
bash tests/integration/scripts/run.sh elasticsearch                  # es8：go + func + load + scale
bash tests/integration/scripts/run.sh elasticsearch heavy            # es8：同一批套件，放大并发与超时
bash tests/integration/scripts/run.sh elasticsearch normal es7       # v7 适配器：go + func
bash tests/integration/scripts/run.sh elasticsearch normal es9       # v9 适配器：go + func
bash tests/integration/scripts/run.sh elasticsearch normal es8 go    # 只跑 Go IT
grep -A2 'TP-E' docs/test/reports/mysql-elasticsearch.md
```

对应 Make 目标：`make it-es`、`make it-es7`、`make it-es9` —— `make it-es PROFILE=heavy` 把写并发与超时放大到压测规模，
`SUITES=go,func` 收窄范围。

| 层级 | 入口 | 报告 |
| --- | --- | --- |
| Integration | `go test -tags esintegration ./connector/sink/elasticsearch/`（真实 ES8） | `go test` 输出 |
| E2E | `sinks/elasticsearch/harness/es_it.py`（TP-E01..） | `docs/test/reports/mysql-elasticsearch.md` |
| Load | `sinks/elasticsearch/harness/es_load_it.py`（TP-EL01..EL05） | `docs/test/reports/mysql-elasticsearch-load.md` |
| Scale | `sinks/elasticsearch/scale/es_scale_it.py`（normal / heavy） | `docs/test/reports/mysql-elasticsearch-scale-{profile}.md` |

隔离与旋钮：

- 每个套件独占一套索引命名空间，命名与清理共用：`ES_INDEX_PREFIX`（默认 `db_tp_es_snk`）、
  `ES_LOAD_INDEX_PREFIX`（默认 `db_tp_es_load_snk`）、`ES_SCALE_INDEX_PREFIX`（默认 `db_tp_scale_snk_*`）。
  `action.destructive_requires_name` 为开，清理逐个索引删除。
- `ES_URL`（默认 `http://127.0.0.1:19200`）、`ES_USERNAME` / `ES_PASSWORD`（一次性节点留空）、
  `ES_REQUEST_TIMEOUT`（秒）。
- 批量参数扫描：TP-EL02 用 `ES_LOAD_BATCH_SIZE` / `ES_LOAD_BATCH_INTERVAL`（默认 `1000` / `500ms`），
  TP-EL03 重启用 `ES_LOAD_SMALL_BATCH` / `ES_LOAD_SMALL_INTERVAL`（默认 `100` / `100ms`）。
  `LOAD_*` 与 MySQL 负载套件共用。
- 规模复用 `SCALE_TOTAL_ROWS` / `SCALE_WORKERS` / `SCALE_TOTAL_TIMEOUT`（默认按档位 `1800` / `5400`）与 `ES_SCALE_RESTART_ROWS`（默认 `100`）。
- `tpl01_typed.c_bit` 在造数后被删除：sink v1 拒绝 `BIT` 列。
- `write-batch-size` / `write-batch-interval` 是 runner 级旋钮：每次 `Sink.Write` 一个 bulk，
  按大小、按间隔、以及任何 split / position / watermark / captured message 时刷写（`composer/runner.go`）。
  快照 chunk 与 binlog 事件走同一条路径，两种都会被扫描覆盖。

## Elasticsearch 7 sink 套件（v7 客户端适配器）

ES7 的 Integration 层与 ES8 是同一个测试二进制，用 `sink.version: 7` 跑在第二个集群上；复用
`es_integration_test.go`，只额外覆盖 ES7 特有部分（适配器接线、无类型 mapping、schema 复核、
close/error 路径）。这一层不需要 MySQL。

ES7 的 E2E 层是 ES8 的 E2E harness（`sinks/elasticsearch/harness/es_it.py`，TP-E01..TP-E10）
换成 `sink.version=7`、`ES_EXPECT_MAJOR=7` 再跑一遍：真实 MySQL 源、快照 + binlog、真实 `go-cdc`
二进制、真实 7.17 节点。这层需要 MySQL，所以光有 ES7 节点不够。

```bash
# Integration + E2E（MySQL + ES7；ES8 节点与 Kafka 不会启动）
bash tests/integration/scripts/run.sh elasticsearch normal es7        # go + func
bash tests/integration/scripts/teardown.sh elasticsearch es7
# 报告：docs/test/reports/mysql-elasticsearch7.md
```

Make 目标：`make it-es7`（`SUITES=go` 或 `SUITES=func` 可收窄）。

| 层级 | 入口 | 报告 |
| --- | --- | --- |
| Integration | `go test -tags esintegration -run TestElasticsearch7 ...`（真实 ES7） | `go test` 输出 |
| E2E | `sinks/elasticsearch/harness/es_it.py` 配 `sink.version=7`（TP-E01..） | `docs/test/reports/mysql-elasticsearch7.md` |

旋钮：`ES7_URL`（默认 `http://127.0.0.1:19201`）、`ES7_IMAGE`、`ES7_HEAP`、`ES7_HOST_PORT`，
外加 ES8 的 E2E 旋钮（`ES_SRC_DB`、`ES_SNK_DB`、`ES_URL`）与 harness 的
`ES_SINK_VERSION` / `ES_EXPECT_MAJOR`。ES8 运行器只过滤 `-run 'TestES'`，所以 ES7 测试不会跑在
ES8 节点上，反之亦然；集群 major 与 `ES_EXPECT_MAJOR` 不符、或 fixture 不再钉住 `sink.version` 时，
E2E harness 立即失败。已验证服务端：Elasticsearch 7.17.x；7.0–7.16 未验证（Go 7.17 客户端要求
`X-Elastic-Product` 响应头，7.14 起才有）。

## Elasticsearch 9 sink 套件（v9 客户端适配器）

ES7 套件在 v9 适配器上的镜像。Integration 层仍是同一个测试二进制，用 `sink.version: 9` 跑在第三个
集群上；复用 `es_integration_test.go`，由 `es9_integration_test.go` 覆盖 ES9 特有部分。这一层不需要
MySQL。E2E 层是 `sinks/elasticsearch/harness/es_it.py`（TP-E01..TP-E10）配 `sink.version=9`、
`ES_EXPECT_MAJOR=9`，因此同样需要 MySQL。

```bash
# Integration + E2E（MySQL + ES9；ES8/ES7 节点与 Kafka 不会启动）
bash tests/integration/scripts/run.sh elasticsearch normal es9        # go + func
bash tests/integration/scripts/teardown.sh elasticsearch es9
# 报告：docs/test/reports/mysql-elasticsearch9.md
```

Make 目标：`make it-es9`。

| 层级 | 入口 | 报告 |
| --- | --- | --- |
| Integration | `go test -tags esintegration -run TestElasticsearch9 ...`（真实 ES9） | `go test` 输出 |
| E2E | `sinks/elasticsearch/harness/es_it.py` 配 `sink.version=9`（TP-E01..） | `docs/test/reports/mysql-elasticsearch9.md` |

旋钮：`ES9_URL`（默认 `http://127.0.0.1:19202`）、`ES9_IMAGE`、`ES9_HEAP`、`ES9_HOST_PORT`，
外加共享的 E2E 旋钮（`ES_SRC_DB`、`ES_SNK_DB`、`ES_URL`）与 `ES_SINK_VERSION` / `ES_EXPECT_MAJOR`。
ES9 节点位于 `es9` compose profile 之后，默认服务列表与 ES7/ES8 job 都不会拉起它。
目标服务端：Elasticsearch 9.5.x（CI job `sink-elasticsearch` 的 `es9` 行）；适配器单测用 `httptest`，不需要集群。
