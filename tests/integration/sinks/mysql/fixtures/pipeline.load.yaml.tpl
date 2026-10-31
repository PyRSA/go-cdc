source:
  type: mysql
  hostname: ${MYSQL_HOST}
  port: ${MYSQL_PORT}
  username: ${SRC_USER}
  password: "${SRC_PASSWORD}"
  tables: '${LOAD_SRC_DB}\.tpl01_(t1|t2|t3|t4|typed|large)'
  server-id: ${CDC_SERVER_ID_LOAD}
  server-time-zone: Asia/Shanghai
  scan.startup.mode: initial
  scan.incremental.snapshot.chunk.size: 4096
  scan.snapshot.fetch.size: 1024
  schema-change.enabled: false
  connect.timeout: 30s

sink:
  type: mysql
  hostname: ${MYSQL_HOST}
  port: ${MYSQL_PORT}
  username: ${SINK_USER}
  password: "${SINK_PASSWORD}"
  auto-create-table: true
  write-batch-size: 1000
  write-batch-interval: 500ms
  max-retries: 3

route:
  - source-table: '${LOAD_SRC_DB}\.tpl01_t1'
    sink-table: ${LOAD_SNK_DB}.tpl01_t1_out
  - source-table: '${LOAD_SRC_DB}\.tpl01_t2'
    sink-table: ${LOAD_SNK_DB}.tpl01_t2_out
  - source-table: '${LOAD_SRC_DB}\.tpl01_t3'
    sink-table: ${LOAD_SNK_DB}.tpl01_t3_out
  - source-table: '${LOAD_SRC_DB}\.tpl01_t4'
    sink-table: ${LOAD_SNK_DB}.tpl01_t4_out
  - source-table: '${LOAD_SRC_DB}\.tpl01_typed'
    sink-table: ${LOAD_SNK_DB}.tpl01_typed_out
  - source-table: '${LOAD_SRC_DB}\.tpl01_large'
    sink-table: ${LOAD_SNK_DB}.tpl01_large_out

transform:
  - source-table: '${LOAD_SRC_DB}\.tpl01_.*'
    projection: '*'
    primary-keys: id

pipeline:
  name: ${PIPELINE_LOAD_NAME}
  parallelism: ${LOAD_PARALLELISM}

checkpoint:
  storage: file
  interval: 3s
  file-path: ${CHECKPOINT_LOAD_DIR}
