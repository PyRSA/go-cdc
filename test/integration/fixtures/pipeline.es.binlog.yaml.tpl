# Binlog-only variant: no initial snapshot, schema changes enabled so the sink
# must reject them. Used by TP-E08 (catalog pre-registration) and TP-E09 (DDL).
source:
  type: mysql
  hostname: ${MYSQL_HOST}
  port: ${MYSQL_PORT}
  username: ${SRC_USER}
  password: "${SRC_PASSWORD}"
  tables: '${ES_SRC_DB}\.tpe01_.*'
  server-id: ${CDC_SERVER_ID_ES_RESTART}
  server-time-zone: Asia/Shanghai
  scan.startup.mode: ${ES_STARTUP_MODE}
  scan.incremental.snapshot.chunk.size: 512
  scan.snapshot.fetch.size: 256
  schema-change.enabled: true
  connect.timeout: 30s

sink:
  type: elasticsearch
  hosts: ["${ES_URL}"]
  auto-create: true
  write-batch-size: 50
  write-batch-interval: 100ms
  max-retries: 3
  request-timeout: 30s

route:
  - source-table: '${ES_SRC_DB}\.tpe01_users'
    sink-table: ${ES_SNK_DB}.tpe01_users
  - source-table: '${ES_SRC_DB}\.tpe01_orders'
    sink-table: ${ES_SNK_DB}.tpe01_orders
  - source-table: '${ES_SRC_DB}\.tpe01_merge_a'
    sink-table: ${ES_SNK_DB}.tpe01_merged
  - source-table: '${ES_SRC_DB}\.tpe01_merge_b'
    sink-table: ${ES_SNK_DB}.tpe01_merged
  - source-table: '${ES_SRC_DB}\.tpe01_composite'
    sink-table: ${ES_SNK_DB}.tpe01_composite
  - source-table: '${ES_SRC_DB}\.tpe01_types'
    sink-table: ${ES_SNK_DB}.tpe01_types
  - source-table: '${ES_SRC_DB}\.tpe01_filtered'
    sink-table: ${ES_SNK_DB}.tpe01_filtered

transform:
  - source-table: '${ES_SRC_DB}\.tpe01_users'
    projection: id, name, city, balance
    primary-keys: id
  - source-table: '${ES_SRC_DB}\.tpe01_merge_.*'
    projection: id, tenant_id, amount, note
    primary-keys: id
  - source-table: '${ES_SRC_DB}\.tpe01_composite'
    projection: '*'
    primary-keys: tenant_id, seq
  - source-table: '${ES_SRC_DB}\.tpe01_filtered'
    projection: '*'
    filter: city = 'hz'
    primary-keys: id
  - source-table: '${ES_SRC_DB}\.tpe01_.*'
    projection: '*'
    primary-keys: id

pipeline:
  name: ${PIPELINE_ES_RESTART_NAME}
  parallelism: 1

checkpoint:
  storage: file
  interval: 2s
  file-path: ${CHECKPOINT_ES_RESTART_DIR}
