# go-cdc load integration test report

- Run time: 2026-09-29 16:44:41
- Source DB: `db_tp_load_src` sink DB: `db_tp_load_snk` (credentials redacted)
## Volume plan (configured knobs)

| Knob | Value | Meaning |
| --- | --- | --- |
| `LOAD_ROWS_PER_TABLE` | 50000 | rows per normal table (t1–t4 + typed) |
| normal tables | 5 | t1,t2,t3,t4,typed |
| normal row total | 250000 | baseline snapshot rows |
| `LOAD_LARGE_ROWS` | 50 | rows in `tpl01_large` |
| `LOAD_LARGE_FIELD_BYTES` | 2097152 (2.00 MiB) | per-field size for `c_img` LONGBLOB and `c_text` LONGTEXT |
| mega payload total | ~200.0 MiB | 50 × 2097152B × 2 fields |
| peak | 90s × 4 workers × batch 200 | incremental stress |

## Phase timings

| Phase | Seconds |
| --- | ---: |
| schema_setup | 0.4 |
| seed_baseline | 12.0 |
| cdc_start | 0.0 |
| cdc_snapshot | 105.4 |
| peak_write | 90.0 |
| cdc_peak_catchup | 1302.0 |
| suite_total | 1513.3 |

- Pass 4 / fail 0

## TP-L01 Multi-table initial sync (t1–t4 + typed + mega-fields)

- Result: ✅ PASS
- Action: 4+typed × 50000 rows; tpl01_large × 50 rows × 2097152B img+text
- Expected: Each table reaches planned row count; src/sink delta <= 1%

| Table | Expected rows | Source rows | Sink rows | Delta | Verdict |
| --- | --- | --- | --- | --- | --- |
| tpl01_t1 | 50000 | 50000 | 50000 | 0.00% | ✅ |
| tpl01_t2 | 50000 | 50000 | 50000 | 0.00% | ✅ |
| tpl01_t3 | 50000 | 50000 | 50000 | 0.00% | ✅ |
| tpl01_t4 | 50000 | 50000 | 50000 | 0.00% | ✅ |
| tpl01_typed | 50000 | 50000 | 50000 | 0.00% | ✅ |
| tpl01_large | 50 | 50 | 50 | 0.00% | ✅ |
| Summary | | | | | **✅ PASS** |

## TP-L03 Typed columns under load (spot-check)

- Result: ✅ PASS
- Action: Compare typed sample ids [1, 25000, 50000] source vs sink
- Expected: tinyint/int/bigint/decimal/varchar/date/datetime/json/enum/bit/bool match

| Check | Expected | Actual | Verdict |
| --- | --- | --- | --- |
| sample rows | 3 | src=3 snk=3 | ✅ |
| values match | equal | equal | ✅ |
| Summary | | | **✅ PASS** |

## TP-L04 Mega-field sync (image LONGBLOB + LONGTEXT 1–5MiB)

- Result: ✅ PASS
- Action: 50 rows × 2097152B JPEG-like c_img + 2097152-char c_text
- Expected: Row counts catch up; LENGTH(c_img)/CHAR_LENGTH(c_text) match; sink image starts with JPEG SOI FFD8

| Check | Expected | Actual | Verdict |
| --- | --- | --- | --- |
| row count | 50 (delta<=1%) | src=50 snk=50 | ✅ |
| c_img LONGBLOB length | 2097152 (2.00 MiB) | 2097152 | ✅ |
| c_text LONGTEXT length | 2097152 chars | 2097152 | ✅ |
| image magic (JPEG SOI) | starts with FFD8 | FFD8FFE0 | ✅ |
| sample LENGTH match src/sink | equal | equal | ✅ |
| Summary | | | **✅ PASS** |

## TP-L02 Peak incremental stress (tpl01_t1 + typed)

- Result: ✅ PASS
- Action: 4 workers x 90s concurrent INSERT tpl01_t1+typed
- Expected: After catch-up delta <=1%; job stays up; resume offset recorded

| Check | Expected | Actual | Verdict |
| --- | --- | --- | --- |
| peak writes | >0 | 2022200 rows / 90.0s ≈ 22462 rps | ✅ |
| tpl01_t1 src/sink delta | <=1% | src=2072200 snk=2070201 lag=0.10% | ✅ |
| typed src/sink delta | <=1% | src=2072200 snk=2070200 lag=0.10% | ✅ |
| job still running | yes | True | ✅ |
| resume offset recorded | yes | yes | ✅ |
| Summary | | | **✅ PASS** |

- workers written: [501200, 507800, 508000, 505200]

