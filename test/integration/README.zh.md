# go-cdc 集成测试（Docker Compose）

本地与 GitHub Actions 一律通过 **Docker Compose** 拉起带 binlog 的 MySQL，再跑：

1. **功能语义** TP-01～TP-13（`harness/run_it.py`）
2. **类型 / 时区** TP-D01～TP-D04（`harness/types_it.py`）
3. **负载 / 峰值** TP-L01～TP-L04（`harness/load_it.py`，tier A）
4. **规模** 多库多表 E2E（`scale/scale_it.py`，配置 `normal` / `heavy`）— 单独执行 [`scale/run-scale.sh`](scale/run-scale.sh)；运行时以 MySQL root 建库（见 [`scale/README.md`](scale/README.md)）

生成报告（英文）落在 `docs/test/reports/`：

| 套件 | 报告 |
|------|------|
| 功能 | `docs/test/reports/mysql-mysql-func.md` |
| 类型 | `docs/test/reports/mysql-mysql-types.md` |
| 负载 | `docs/test/reports/mysql-mysql-load.md` |
| 规模 | `docs/test/reports/mysql-mysql-scale-{normal\|heavy}.md` |

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
bash test/integration/scripts/mysql-down.sh
```

## 规模套件

MySQL 已启动后单独跑（不包含功能/类型/负载）：

```bash
bash test/integration/scale/run-scale.sh normal
# 或
make integration-scale-heavy
```

规模库（`db_tp_scale_src_NN` / `db_tp_scale_snk_NN`）由 `scale_it.py` 在运行时以 compose root（`ROOT_USER` / `ROOT_PASSWORD`）创建，再授权给与其他套件相同的匿名源/目标账号。

## 负载 knobs

| 变量 | 默认 | 说明 |
|------|------|------|
| `LOAD_ROWS_PER_TABLE` | `50000`（本地默认）；CI：`30000` normal / `100000` full | 普通表行数（t1–t4 + typed） |
| `LOAD_LARGE_ROWS` | `50` | 大字段表 `tpl01_large` 行数 |
| `LOAD_LARGE_FIELD_BYTES` | `2097152`（2MiB，限制在 1–5MiB） | 单字段大小：图片 `LONGBLOB` + `LONGTEXT` |
| `LOAD_PEAK_SECONDS` | `90`（本地）；CI：MySQL 5.7 为 `45` / 8.0 为 `90` | 峰值写入时长 |
| `LOAD_PEAK_WORKERS` | `4`（本地与 CI 两版本相同） | 峰值写并发；**造数**用 `workers × 2`（上限 16，且不超过表数） |
| `LOAD_PARALLELISM` | 默认同 `LOAD_PEAK_WORKERS` | 负载套件 CDC `pipeline.parallelism` |
| `LOAD_PEAK_BATCH` | `200` | 每批 INSERT 行数 |
| `LOAD_PROFILE` | `run-e2e.sh` 默认 `local`；CI 为 `full` / `normal` | 分档标签（写入 load 报告） |

负载用例：TP-L01 多表全量（t1–t4 + typed + large）、TP-L02 峰值（t1 + typed）、TP-L03 类型抽检、TP-L04 大字段 LENGTH 校验。

## GitHub Actions

[`.github/workflows/integration.yml`](../../.github/workflows/integration.yml)（**Go CDC CI**）。任意分支 `push`、任意 `pull_request`、`workflow_dispatch`。

| Job | 矩阵 | 内容 |
| --- | --- | --- |
| `go-check` | — | fmt / lint / `make test` / build / **scale 单测**（`pytest scale/test_*.py`） |
| `mysql-mysql-e2e` | MySQL **5.7** + **8.0** | 功能 + 类型 + 负载 |
| `mysql-scale-e2e` | 仅 MySQL **8.0** | scale `normal` / `heavy`（独立 compose 项目，宿主机端口 `13307`） |

**负载分档**（`LOAD_ROWS_PER_TABLE`）：PR / `main` / `master` / `release-*` → **100000**（`LOAD_PROFILE=full`）；其他分支 push → **30000**（`normal`）。峰值：5.7 → 45s×4；8.0 → 90s×4。

**Scale 分档**：PR / `main` / `master` / `release-*` → **heavy**（10 库 / 50 表 / ~30 万行 / w=8）；其他分支 push → **normal**（5 库 / 20 表 / ~5 万行 / w=4）；`workflow_dispatch` 可选 `normal`/`heavy`（默认 heavy）。
