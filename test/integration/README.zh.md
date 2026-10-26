# go-cdc 集成测试（Docker Compose）

本地与 GitHub Actions 一律通过 **Docker Compose** 拉起带 binlog 的 MySQL，再跑：

1. **功能语义** TP-01～TP-13（`harness/run_it.py`）
2. **类型 / 时区** TP-D01～TP-D04（`harness/types_it.py`）
3. **负载 / 峰值** TP-L01～TP-L04（`harness/load_it.py`，tier A）
4. **规模** 多库多表 E2E（`scale/scale_it.py`，配置 `normal` / `heavy`）— 单独执行 [`scale/run-scale.sh`](scale/run-scale.sh)；运行时以 MySQL root 建库（见 [`scale/README.md`](scale/README.md)）
5. **Kafka sink** TP-K01～K08（`harness/kafka_it.py`）— MySQL + Kafka 就绪后跑 [`scripts/run-kafka-e2e.sh`](scripts/run-kafka-e2e.sh)
6. **Doris sink** MySQL→Doris 流水线 TP-DR01～DR07（功能 + 负载对齐，`harness/doris_it.py`）+ sink 级 Go 套件（`doris/`，tag `doris_integration`）— MySQL + Doris 就绪后跑 [`scripts/run-doris-e2e.sh`](scripts/run-doris-e2e.sh)
7. **Doris 规模** 对 Doris 的多库多表压测（`scale/doris_scale_it.py`，配置 `normal` / `heavy`）— 跑 [`scripts/run-doris-scale.sh`](scripts/run-doris-scale.sh)

生成报告（英文）落在 `docs/test/reports/`：

| 套件 | 报告 |
|------|------|
| 功能 | `docs/test/reports/mysql-mysql-func.md` |
| 类型 | `docs/test/reports/mysql-mysql-types.md` |
| 负载 | `docs/test/reports/mysql-mysql-load.md` |
| 规模 | `docs/test/reports/mysql-mysql-scale-{normal\|heavy}.md` |
| Kafka | `docs/test/reports/mysql-kafka.md` |
| Doris | `docs/test/reports/mysql-doris.md`（含功能 + 负载阶段；Go 套件日志见 `workdir/doris-integration.log`） |
| Doris 规模 | `docs/test/reports/mysql-doris-scale-{normal\|heavy}.md` |

说明文档：[`docs/test/TYPES.zh.md`](../../docs/test/TYPES.zh.md)（不是跑测报告）。

English: [README.md](./README.md)

## 命名约定（匿名账号 + 按测试点）

| 类别 | 命名 | 示例 |
|------|------|------|
| 源/目标账号 | 匿名固定 token | `u_a7k9m2x4` / `u_b3j6c8y1` |
| 库名 | `db_tp_{suite}_{src\|snk}` | `db_tp_func_src` |
| 功能表 | `tpNN_*` | `tp01_rows` |
| 类型表 | `tpdNN_*` | `tpd01_types` |
| 负载表 | `tplNN_*` | `tpl01_t1`、`tpl01_typed`、`tpl01_large` |
| 目标表 | 源表名 + `_out` | `tp01_rows_out` |

## 快速跑

```bash
bash test/integration/scripts/mysql-up.sh
bash test/integration/scripts/run-e2e.sh
ls docs/test/reports/
```

`run-e2e.sh` / `run-scale.sh` **仅在本地**退出时删 compose MySQL（`KEEP_IT_MYSQL=1` 可保留）。**CI 不在 harness 退出时清理**，只由 workflow **最后一步 Tear down**（`if: always()`，在上传报告之后）执行 `mysql-down`，避免报告/产物未上传就先清掉环境。

## 规模套件

MySQL 已启动后单独跑（不包含功能/类型/负载）：

```bash
bash test/integration/scale/run-scale.sh normal
# 或
make integration-scale-heavy
```

规模库（`db_tp_scale_src_NN` / `db_tp_scale_snk_NN`）由 `scale_it.py` 在运行时以 compose root（`ROOT_USER` / `ROOT_PASSWORD`）创建，再授权给与其他套件相同的匿名源/目标账号。

### Doris 变体

同一套 profile、同样的表/行分配，目标换成 Doris：

```bash
bash test/integration/scripts/mysql-up.sh
bash test/integration/scripts/doris-up.sh
bash test/integration/scripts/run-doris-scale.sh normal
# 或：bash test/integration/scripts/run-doris-scale.sh heavy
# 或：make integration-doris-scale-normal / integration-doris-scale-heavy
```

它**故意不与** MySQL→MySQL 规模套件共用库名（改用
`db_tp_dscale_src_NN` / `db_tp_dscale_snk_NN`），两者可并行而不会互删对方表。
三处差异由 Doris v1 语义决定，报告会逐条写明：

| 差异 | 原因 |
| --- | --- |
| `delete` 事件组的表被重映射为 `insert_update` | Doris sink 拒绝 DELETE，`delete.enabled: true` 在解析期即报错 |
| `wide` 去掉 `c_time TIME`；`large` 用 `payload LONGTEXT` 取代 `binary_data BLOB` | `types.go` 在 v1 中没有 TIME / 二进制类型的映射 |
| 目标库由 `auto-create-table` 建立 | sink 自己执行 `CREATE DATABASE IF NOT EXISTS` |

内容校验在启动时探测：MySQL 与 Doris 对同一固定字符串的 `CRC32` 必须相同，
否则降级为字符长度求和（MySQL 用 `CHAR_LENGTH`，Doris 用 `LENGTH`），并在报告
顶部标注已降级，而不是静默比较两种不同的聚合值。

## 负载 knobs

| 变量 | 默认 | 说明 |
|------|------|------|
| `LOAD_ROWS_PER_TABLE` | `50000`（本地默认）；CI：`30000` normal / `100000` full | 普通表行数（t1–t4 + typed） |
| `LOAD_LARGE_ROWS` | `50` | 大字段表 `tpl01_large` 行数 |
| `LOAD_LARGE_FIELD_BYTES` | `2097152`（2MiB，限制在 1–5MiB） | 单字段大小：图片 `LONGBLOB` + `LONGTEXT` |
| `LOAD_PEAK_SECONDS` | `60`（本地与 CI MySQL 5.7 / 8.0 相同） | 峰值写入时长 |
| `LOAD_PEAK_WORKERS` | `4`（本地与 CI 两版本相同） | 峰值写并发；**造数**用 `workers × 2`（上限 16，且不超过表数） |
| `LOAD_PARALLELISM` | 默认同 `LOAD_PEAK_WORKERS` | 负载套件 CDC `pipeline.parallelism` |
| `LOAD_PEAK_BATCH` | `200` | 每批 INSERT 行数 |
| `LOAD_PROFILE` | `run-e2e.sh` 默认 `local`；CI 为 `full` / `normal` | 分档标签（写入 load 报告） |

负载用例：TP-L01 多表全量（t1–t4 + typed + large）、TP-L02 峰值（t1 + typed）、TP-L03 类型抽检、TP-L04 大字段 LENGTH 校验。

## GitHub Actions

[`.github/workflows/integration.yml`](../../.github/workflows/integration.yml)（界面名 **`CI`**）。仅在 CDC 源码、模块、`test/integration/**`（或本 workflow）变更时的 `push` / `pull_request`，以及 `workflow_dispatch`。任意 `*.md` 与样例 `configs/**` 不触发。检查项形如 `CI / Integration / … (push)`。

仓库卫生：[`repo-checks.yml`](../../.github/workflows/repo-checks.yml) → 每次 push/PR 的 `CI / Repo / File size`、`CI / Repo / License`。

| Job | 矩阵 | 内容 |
| --- | --- | --- |
| `go-check` | — | fmt / lint / `make test` / build / **scale 单测**（`pytest scale/`） |
| `mysql-mysql-e2e` | MySQL **5.7** + **8.0** | 功能 + 类型 + 负载；**8.0 另跑 Kafka sink** TP-K01..K08 |
| `mysql-scale-e2e` | 仅 MySQL **8.0** | scale `normal` / `heavy`（独立 compose 项目，宿主机端口 `13307`） |
| `doris-e2e` | MySQL **8.0** + Doris **2.1.8** | MySQL→Doris 流水线 TP-DR01..DR07（功能 + 负载）+ sink Go 套件 + Doris 规模（[`doris-integration.yml`](../../.github/workflows/doris-integration.yml)） |
| `elasticsearch-e2e` | MySQL **8.0** + compose ES8 | ES sink：Go IT（`esintegration`）+ E2E + 负载 + scale `normal` |

**负载分档**（`LOAD_ROWS_PER_TABLE`）：PR / `main` / `master` / `release-*` → **100000**（`LOAD_PROFILE=full`）；其他分支 push → **30000**（`normal`）。峰值：5.7 / 8.0 均为 **60s×4**。Doris `doris-e2e` 使用同一分档（5 张批量表）；`workflow_dispatch` 可显式选 `normal` / `full`。

**Scale 分档**：PR / `main` / `master` / `release-*` → **heavy**（10 库 / 50 表 / ~30 万行 / w=8，含热点 burst+peak）；其他分支 push → **normal**（5 库 / 20 表 / ~5 万行 / w=4）；`workflow_dispatch` 可选 `normal`/`heavy`（默认 heavy）。`mysql-scale-e2e` job 超时 **120 分钟**。

**Doris 规模分档**（在 `doris-e2e` 内）：PR / 分支 push → **normal**（5 库 / 20 表 / ~5 万行 / w=4）；`workflow_dispatch` 且 `load_profile=full` → **heavy**（10 库 / 50 表 / ~30 万行 / w=8，含热点 burst+peak）。`doris-e2e` job 超时 **150 分钟**。

**Artifact**：仅在 push 到 `main` / `master` / `release-*` 时上传（PR 与其它分支不上传）；同名旧 artifact 会先删再传，只留最新一份；其余仍按仓库默认约 90 天过期。

## Doris sink 套件

需要 Linux、Docker Compose、host 网络、mysql 客户端，以及 FE/BE 内核参数：

```bash
sudo sysctl -w fs.inotify.max_user_watches=1048576
sudo sysctl -w fs.inotify.max_user_instances=8192
sudo sysctl -w vm.max_map_count=2000000
```

`doris-up.sh` 在参数不足时直接拒绝启动（CI 上无免密 sudo 自动调高）。未设置
`DORIS_HOST` 时 Go sink 套件自动跳过，普通 `go test ./...` 不会启动 Doris。

```bash
# make integration-doris-up  # mysql-up + doris-up
bash test/integration/scripts/mysql-up.sh
bash test/integration/scripts/doris-up.sh
bash test/integration/scripts/run-doris-e2e.sh
# 报告：docs/test/reports/mysql-doris.md
# Go sink 日志：test/integration/workdir/doris-integration.log
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

负载阶段（DR05–DR07）复用与 mysql→mysql / kafka 负载套件相同的 `LOAD_*` 旋钮
与 CI 分档。Doris v1 拒绝 BLOB/ENUM/BIT/SET/TIME，因此批量表放在
`db_tp_load_src.tpdl01_*`，只使用可映射类型；`tpl01_large` 的大字段用例没有
Doris 对应项。

同一 `run-doris-e2e.sh` 随后跑 sink 级 Go 套件（`doris_integration`）：自动建表 /
UNIQUE KEY、同批重复主键、空 Schema 缓存重启、额外列、类型拒绝/往返、Label +
`SHOW LOAD`（重复 Label 不得追加第二行、过滤行为、已完成 Label 的重试；若该构
建根本不记录 Stream Load 作业，只有“每个 Label 恰好一行”这个子测试报 `SKIP`，
该探针其余断言照常执行）、`DESC`
可空性与空串默认值解析、`ADD COLUMN` 可见性与“部分列已添加”状态、重启重新校验、
多表首败即停。`Publish Timeout` / `CANCELLED` / 5xx 注入仍由单元 / `httptest`
覆盖；“DDL 已生效但返回错误”也属于该范围。

独立 workflow [`doris-integration.yml`](../../.github/workflows/doris-integration.yml)
会启动 MySQL + Doris `2.1.8` FE+BE、调高内核参数、执行套件（功能 TP-DR01..DR04，
再跑负载 TP-DR05..DR07，最后跑 Doris 规模套件），并上传 Markdown 报告与
`cdc-doris*.log`、compose 日志。
## Elasticsearch 8 sink 套件

ES8 sink 与 MySQL sink 对齐的四层测试。共用 MySQL compose、造数器（`harness/load_it.py`、`scale/scale_it.py`）与同名 knob，差异只在目标端：`harness/es_client.py`（HTTP）、`harness/es_verify.py`（独立 oracle）。

```bash
bash test/integration/scripts/mysql-up.sh
bash test/integration/scripts/es-up.sh          # compose 服务 elasticsearch（8.15.3）

bash test/integration/scripts/run-es-integration.sh   # Go sink/runner IT（tag esintegration）
bash test/integration/scripts/run-es-e2e.sh           # MySQL binlog -> runner -> ES8
bash test/integration/scripts/run-es-load.sh          # 每表 5 万行 + 峰值写入
bash test/integration/scripts/run-es-scale.sh normal  # 5 库 / 20 表
bash test/integration/scripts/run-es-scale.sh heavy   # 10 库 / 50 表 + 热点 burst/peak
```

对应 Make 目标：`make integration-es-up`、`integration-es-integration`、`integration-es-e2e`、`integration-es-load`、`integration-es-scale-normal`、`integration-es-scale-heavy`（`test/integration/Makefile` 中另有短写 `make es-*`）。

| 层级 | 入口 | 报告 |
| --- | --- | --- |
| Integration | `go test -tags esintegration ./connector/sink/elasticsearch/`（真实 ES8） | `go test` 输出 |
| E2E | `harness/es_it.py`（TP-E01..） | `docs/test/reports/mysql-elasticsearch.md` |
| Load | `harness/es_load_it.py`（TP-EL01..EL05） | `docs/test/reports/mysql-elasticsearch-load.md` |
| Scale | `scale/es_scale_it.py`（normal / heavy） | `docs/test/reports/mysql-elasticsearch-scale-{profile}.md` |

隔离与 knob：

- 每个套件有独立索引命名空间，既是命名前缀也是清理前缀：`ES_INDEX_PREFIX`（默认 `db_tp_es_snk`）、`ES_LOAD_INDEX_PREFIX`（默认 `db_tp_es_load_snk`）、`ES_SCALE_INDEX_PREFIX`（默认 `db_tp_scale_snk_*`）。compose 开了 `action.destructive_requires_name`，清理时逐索引删除。
- `ES_URL`（默认 `http://127.0.0.1:19200`）、`ES_USERNAME` / `ES_PASSWORD`（一次性节点为空）、`ES_REQUEST_TIMEOUT`（秒）。
- 负载批量扫描：TP-EL02 用 `ES_LOAD_BATCH_SIZE` / `ES_LOAD_BATCH_INTERVAL`（默认 `1000` / `500ms`），TP-EL03 重启用 `ES_LOAD_SMALL_BATCH` / `ES_LOAD_SMALL_INTERVAL`（默认 `100` / `100ms`）；`LOAD_*` 与 MySQL 负载套件共用。
- 规模复用 `SCALE_TOTAL_ROWS` / `SCALE_WORKERS` / `SCALE_TOTAL_TIMEOUT`（默认 `1800`）与 `ES_SCALE_RESTART_ROWS`（默认 `100`）。
- 造数后删除 `tpl01_typed.c_bit`：sink v1 明确拒绝 BIT 列。
- `write-batch-size` / `write-batch-interval` 是 runner 级 knob：一次 `Sink.Write` 对应一个 bulk，按大小、按间隔、以及任何 split / position / watermark / captured 消息触发 flush（`composer/runner.go`），快照分块与 binlog 事件走同一条路径。

校验原则：不把 bulk HTTP 200 当数据正确性证据；`_count` 只做进度门槛，内容一致性用分页（scroll，1000 条/页）流式算出 `(COUNT, SUM(id), SUM(CRC32))` 与 MySQL 比对，并额外做整文档字段比对（NULL / DECIMAL / JSON / 二进制 / 日期时间）。
