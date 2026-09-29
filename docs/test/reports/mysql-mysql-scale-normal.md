# go-cdc scale integration test report (normal)

- Run time: 2026-09-29 16:15:28
- Result: ✅ PASS
- Profile: `normal`
- Workers / parallelism: 4
- Tables: 20
- Total rows (planned seed): 50000
- Source DBs (5): `db_tp_scale_src_01, db_tp_scale_src_02, db_tp_scale_src_03, db_tp_scale_src_04, db_tp_scale_src_05`
- Sink DBs (5): `db_tp_scale_snk_01, db_tp_scale_snk_02, db_tp_scale_snk_03, db_tp_scale_snk_04, db_tp_scale_snk_05`

## Event groups

| Group | Tables |
| --- | ---: |
| insert | 4 |
| update | 4 |
| delete | 4 |
| insert_update | 4 |
| idle | 4 |

## Phase timings

| Phase | Seconds |
| --- | ---: |
| seed_baseline | 2.0 |
| cdc_snapshot | 30.4 |
| mutate | 0.2 |
| cdc_catchup | 15.5 |

- Failures: 0

## Summary

All 20 source/sink pairs matched (COUNT + SUM(id) + name_crc) after snapshot and post-mutation catch-up.

## Tables

| ordinal | src | sink | template | rows | event_group |
| ---: | --- | --- | --- | ---: | --- |
| 1 | `db_tp_scale_src_01.tp_scale_001` | `db_tp_scale_snk_01.tp_scale_001_out` | basic | 4444 | insert |
| 2 | `db_tp_scale_src_01.tp_scale_002` | `db_tp_scale_snk_01.tp_scale_002_out` | wide | 2222 | update |
| 3 | `db_tp_scale_src_01.tp_scale_003` | `db_tp_scale_snk_01.tp_scale_003_out` | indexed | 2222 | delete |
| 4 | `db_tp_scale_src_01.tp_scale_004` | `db_tp_scale_snk_01.tp_scale_004_out` | large | 1111 | insert_update |
| 5 | `db_tp_scale_src_01.tp_scale_005` | `db_tp_scale_snk_01.tp_scale_005_out` | basic | 4444 | idle |
| 6 | `db_tp_scale_src_01.tp_scale_006` | `db_tp_scale_snk_01.tp_scale_006_out` | wide | 2222 | insert |
| 7 | `db_tp_scale_src_01.tp_scale_007` | `db_tp_scale_snk_01.tp_scale_007_out` | indexed | 2222 | update |
| 8 | `db_tp_scale_src_01.tp_scale_008` | `db_tp_scale_snk_01.tp_scale_008_out` | large | 1111 | delete |
| 9 | `db_tp_scale_src_02.tp_scale_009` | `db_tp_scale_snk_02.tp_scale_009_out` | basic | 4444 | insert_update |
| 10 | `db_tp_scale_src_02.tp_scale_010` | `db_tp_scale_snk_02.tp_scale_010_out` | wide | 2222 | idle |
| 11 | `db_tp_scale_src_02.tp_scale_011` | `db_tp_scale_snk_02.tp_scale_011_out` | indexed | 2222 | insert |
| 12 | `db_tp_scale_src_02.tp_scale_012` | `db_tp_scale_snk_02.tp_scale_012_out` | large | 1111 | update |
| 13 | `db_tp_scale_src_02.tp_scale_013` | `db_tp_scale_snk_02.tp_scale_013_out` | basic | 4444 | delete |
| 14 | `db_tp_scale_src_03.tp_scale_014` | `db_tp_scale_snk_03.tp_scale_014_out` | wide | 2222 | insert_update |
| 15 | `db_tp_scale_src_03.tp_scale_015` | `db_tp_scale_snk_03.tp_scale_015_out` | indexed | 2222 | idle |
| 16 | `db_tp_scale_src_03.tp_scale_016` | `db_tp_scale_snk_03.tp_scale_016_out` | large | 1111 | insert |
| 17 | `db_tp_scale_src_04.tp_scale_017` | `db_tp_scale_snk_04.tp_scale_017_out` | basic | 4449 | update |
| 18 | `db_tp_scale_src_04.tp_scale_018` | `db_tp_scale_snk_04.tp_scale_018_out` | wide | 2222 | delete |
| 19 | `db_tp_scale_src_05.tp_scale_019` | `db_tp_scale_snk_05.tp_scale_019_out` | indexed | 2222 | insert_update |
| 20 | `db_tp_scale_src_05.tp_scale_020` | `db_tp_scale_snk_05.tp_scale_020_out` | large | 1111 | idle |
