source:
  type: mysql
  hostname: ${MYSQL_HOST}
  port: ${MYSQL_PORT}
  username: ${SRC_USER}
  password: "${SRC_PASSWORD}"
  tables: '${TYPES_SRC_DB}\.tpd01_types'
  server-id: ${CDC_SERVER_ID_TYPES}
  server-time-zone: Asia/Shanghai
  scan.startup.mode: initial
  scan.incremental.snapshot.chunk.size: 2
  scan.snapshot.fetch.size: 2
  schema-change.enabled: true
  connect.timeout: 30s

sink:
  type: mysql
  hostname: ${MYSQL_HOST}
  port: ${MYSQL_PORT}
  username: ${SINK_USER}
  password: "${SINK_PASSWORD}"
  auto-create-table: true
  write-batch-size: 100
  write-batch-interval: 500ms
  max-retries: 3

route:
  - source-table: '${TYPES_SRC_DB}\.tpd01_types'
    sink-table: ${TYPES_SNK_DB}.tpd01_types_out

transform:
  - source-table: '${TYPES_SRC_DB}\.tpd01_types'
    projection: '*'
    primary-keys: id

pipeline:
  name: ${PIPELINE_TYPES_NAME}
  parallelism: 1

checkpoint:
  storage: file
  interval: 2s
  file-path: ${CHECKPOINT_TYPES_DIR}
