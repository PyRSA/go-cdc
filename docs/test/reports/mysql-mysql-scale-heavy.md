# go-cdc scale integration test report (heavy)

- Run time: 2026-09-29 16:19:13
- Result: ✅ PASS
- Profile: `heavy`
- Workers / parallelism: 8
- Tables: 50
- Total rows (planned seed): 300000
- Source DBs (10): `db_tp_scale_src_01, db_tp_scale_src_02, db_tp_scale_src_03, db_tp_scale_src_04, db_tp_scale_src_05, db_tp_scale_src_06, db_tp_scale_src_07, db_tp_scale_src_08, db_tp_scale_src_09, db_tp_scale_src_10`
- Sink DBs (10): `db_tp_scale_snk_01, db_tp_scale_snk_02, db_tp_scale_snk_03, db_tp_scale_snk_04, db_tp_scale_snk_05, db_tp_scale_snk_06, db_tp_scale_snk_07, db_tp_scale_snk_08, db_tp_scale_snk_09, db_tp_scale_snk_10`

## Event groups

| Group | Tables |
| --- | ---: |
| insert | 10 |
| update | 10 |
| delete | 10 |
| insert_update | 10 |
| idle | 10 |

## Phase timings

| Phase | Seconds |
| --- | ---: |
| seed_baseline | 10.6 |
| cdc_snapshot | 184.1 |
| mutate | 6.9 |
| cdc_catchup | 17.2 |

- Failures: 0

## Summary

All 50 source/sink pairs matched (COUNT + SUM(id) + name_crc) after snapshot and post-mutation catch-up.

## Tables

| ordinal | src | sink | template | rows | event_group |
| ---: | --- | --- | --- | ---: | --- |
| 1 | `db_tp_scale_src_01.tp_scale_001` | `db_tp_scale_snk_01.tp_scale_001_out` | basic | 10526 | insert |
| 2 | `db_tp_scale_src_01.tp_scale_002` | `db_tp_scale_snk_01.tp_scale_002_out` | wide | 5263 | update |
| 3 | `db_tp_scale_src_01.tp_scale_003` | `db_tp_scale_snk_01.tp_scale_003_out` | indexed | 5263 | delete |
| 4 | `db_tp_scale_src_01.tp_scale_004` | `db_tp_scale_snk_01.tp_scale_004_out` | large | 2632 | insert_update |
| 5 | `db_tp_scale_src_01.tp_scale_005` | `db_tp_scale_snk_01.tp_scale_005_out` | basic | 10526 | idle |
| 6 | `db_tp_scale_src_01.tp_scale_006` | `db_tp_scale_snk_01.tp_scale_006_out` | wide | 5263 | insert |
| 7 | `db_tp_scale_src_01.tp_scale_007` | `db_tp_scale_snk_01.tp_scale_007_out` | indexed | 5263 | update |
| 8 | `db_tp_scale_src_01.tp_scale_008` | `db_tp_scale_snk_01.tp_scale_008_out` | large | 2632 | delete |
| 9 | `db_tp_scale_src_01.tp_scale_009` | `db_tp_scale_snk_01.tp_scale_009_out` | basic | 10526 | insert_update |
| 10 | `db_tp_scale_src_01.tp_scale_010` | `db_tp_scale_snk_01.tp_scale_010_out` | wide | 5263 | idle |
| 11 | `db_tp_scale_src_01.tp_scale_011` | `db_tp_scale_snk_01.tp_scale_011_out` | indexed | 5263 | insert |
| 12 | `db_tp_scale_src_01.tp_scale_012` | `db_tp_scale_snk_01.tp_scale_012_out` | large | 2632 | update |
| 13 | `db_tp_scale_src_01.tp_scale_013` | `db_tp_scale_snk_01.tp_scale_013_out` | basic | 10526 | delete |
| 14 | `db_tp_scale_src_01.tp_scale_014` | `db_tp_scale_snk_01.tp_scale_014_out` | wide | 5263 | insert_update |
| 15 | `db_tp_scale_src_01.tp_scale_015` | `db_tp_scale_snk_01.tp_scale_015_out` | indexed | 5263 | idle |
| 16 | `db_tp_scale_src_02.tp_scale_016` | `db_tp_scale_snk_02.tp_scale_016_out` | large | 2632 | insert |
| 17 | `db_tp_scale_src_02.tp_scale_017` | `db_tp_scale_snk_02.tp_scale_017_out` | basic | 10526 | update |
| 18 | `db_tp_scale_src_02.tp_scale_018` | `db_tp_scale_snk_02.tp_scale_018_out` | wide | 5263 | delete |
| 19 | `db_tp_scale_src_02.tp_scale_019` | `db_tp_scale_snk_02.tp_scale_019_out` | indexed | 5263 | insert_update |
| 20 | `db_tp_scale_src_02.tp_scale_020` | `db_tp_scale_snk_02.tp_scale_020_out` | large | 2632 | idle |
| 21 | `db_tp_scale_src_02.tp_scale_021` | `db_tp_scale_snk_02.tp_scale_021_out` | basic | 10526 | insert |
| 22 | `db_tp_scale_src_02.tp_scale_022` | `db_tp_scale_snk_02.tp_scale_022_out` | wide | 5263 | update |
| 23 | `db_tp_scale_src_02.tp_scale_023` | `db_tp_scale_snk_02.tp_scale_023_out` | indexed | 5263 | delete |
| 24 | `db_tp_scale_src_02.tp_scale_024` | `db_tp_scale_snk_02.tp_scale_024_out` | large | 2632 | insert_update |
| 25 | `db_tp_scale_src_02.tp_scale_025` | `db_tp_scale_snk_02.tp_scale_025_out` | basic | 10526 | idle |
| 26 | `db_tp_scale_src_03.tp_scale_026` | `db_tp_scale_snk_03.tp_scale_026_out` | wide | 5263 | insert |
| 27 | `db_tp_scale_src_03.tp_scale_027` | `db_tp_scale_snk_03.tp_scale_027_out` | indexed | 5263 | update |
| 28 | `db_tp_scale_src_03.tp_scale_028` | `db_tp_scale_snk_03.tp_scale_028_out` | large | 2632 | delete |
| 29 | `db_tp_scale_src_03.tp_scale_029` | `db_tp_scale_snk_03.tp_scale_029_out` | basic | 10526 | insert_update |
| 30 | `db_tp_scale_src_03.tp_scale_030` | `db_tp_scale_snk_03.tp_scale_030_out` | wide | 5263 | idle |
| 31 | `db_tp_scale_src_03.tp_scale_031` | `db_tp_scale_snk_03.tp_scale_031_out` | indexed | 5263 | insert |
| 32 | `db_tp_scale_src_03.tp_scale_032` | `db_tp_scale_snk_03.tp_scale_032_out` | large | 2632 | update |
| 33 | `db_tp_scale_src_04.tp_scale_033` | `db_tp_scale_snk_04.tp_scale_033_out` | basic | 10526 | delete |
| 34 | `db_tp_scale_src_04.tp_scale_034` | `db_tp_scale_snk_04.tp_scale_034_out` | wide | 5263 | insert_update |
| 35 | `db_tp_scale_src_04.tp_scale_035` | `db_tp_scale_snk_04.tp_scale_035_out` | indexed | 5263 | idle |
| 36 | `db_tp_scale_src_04.tp_scale_036` | `db_tp_scale_snk_04.tp_scale_036_out` | large | 2632 | insert |
| 37 | `db_tp_scale_src_04.tp_scale_037` | `db_tp_scale_snk_04.tp_scale_037_out` | basic | 10526 | update |
| 38 | `db_tp_scale_src_04.tp_scale_038` | `db_tp_scale_snk_04.tp_scale_038_out` | wide | 5263 | delete |
| 39 | `db_tp_scale_src_05.tp_scale_039` | `db_tp_scale_snk_05.tp_scale_039_out` | indexed | 5263 | insert_update |
| 40 | `db_tp_scale_src_05.tp_scale_040` | `db_tp_scale_snk_05.tp_scale_040_out` | large | 2632 | idle |
| 41 | `db_tp_scale_src_05.tp_scale_041` | `db_tp_scale_snk_05.tp_scale_041_out` | basic | 10526 | insert |
| 42 | `db_tp_scale_src_05.tp_scale_042` | `db_tp_scale_snk_05.tp_scale_042_out` | wide | 5263 | update |
| 43 | `db_tp_scale_src_06.tp_scale_043` | `db_tp_scale_snk_06.tp_scale_043_out` | indexed | 5263 | delete |
| 44 | `db_tp_scale_src_06.tp_scale_044` | `db_tp_scale_snk_06.tp_scale_044_out` | large | 2632 | insert_update |
| 45 | `db_tp_scale_src_06.tp_scale_045` | `db_tp_scale_snk_06.tp_scale_045_out` | basic | 10526 | idle |
| 46 | `db_tp_scale_src_07.tp_scale_046` | `db_tp_scale_snk_07.tp_scale_046_out` | wide | 5263 | insert |
| 47 | `db_tp_scale_src_07.tp_scale_047` | `db_tp_scale_snk_07.tp_scale_047_out` | indexed | 5263 | update |
| 48 | `db_tp_scale_src_08.tp_scale_048` | `db_tp_scale_snk_08.tp_scale_048_out` | large | 2632 | delete |
| 49 | `db_tp_scale_src_09.tp_scale_049` | `db_tp_scale_snk_09.tp_scale_049_out` | basic | 10529 | insert_update |
| 50 | `db_tp_scale_src_10.tp_scale_050` | `db_tp_scale_snk_10.tp_scale_050_out` | wide | 5263 | idle |
