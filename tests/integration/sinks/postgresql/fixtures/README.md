# Why `sinks/postgresql/fixtures/` is (almost) empty

This directory is intentionally kept without data files: the PostgreSQL Pipeline
YAML templates, the source-table DDL and the seed rows are all generated
programmatically at run time by `pg_it.py` / `pg_load_it.py`.

In detail:

- **Pipeline YAML**: the renderer in `harness/pg_it.py` builds the file line by
  line from the current session's `TableSpec`, `RENAMES` and `TRANSFORMS`
  (written to `workdir/pipeline.pg.yaml` and `workdir/pipeline.pg.load.yaml`).
  Every test point uses a different table set, target-table rename, transform and
  schema-evolution switch, so a template would collapse into a large pile of
  conditionals; generating the file is the simpler contract.
- **Source DDL and data**: also inlined in the test-point definitions of
  `pg_it.py` / `pg_load_it.py`. Each test point starts by rebuilding the source
  table with `DROP TABLE IF EXISTS` + `CREATE TABLE` (from `TableSpec.ddl`) and
  drops the routed target tables, so no state leaks between test points and a
  single test point can be re-run with meaning.
- **`run.sh` seeds nothing here**: in
  `tests/integration/scripts/run.sh`, the seed branch groups
  `elasticsearch | postgresql` as "the suite creates its own source databases"
  and only calls `reset_checkpoints()`, never `seed_sql()`.

For contrast, `sinks/mysql/`, `sinks/kafka/` and `sinks/doris/` need one
**shared static source snapshot** (`seed_*.sql`) that `run.sh` loads once and the
func/load suites share, which is why they ship SQL files. `sinks/elasticsearch/`
generates its data inline in Python for the same reason as here and keeps only
the pipeline templates.

Do not add `seed_*.sql` or `*.yaml.tpl` here unless the matching harness has been
changed to read them.
