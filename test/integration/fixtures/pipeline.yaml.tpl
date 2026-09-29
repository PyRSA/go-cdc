source:
  type: mysql
  hostname: ${MYSQL_HOST}
  port: ${MYSQL_PORT}
  username: ${SRC_USER}
  password: "${SRC_PASSWORD}"
  tables: '${FUNC_SRC_DB}\.tp(01_rows|02_keys|07_fail)'
  server-id: ${CDC_SERVER_ID}
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
  - source-table: '${FUNC_SRC_DB}\.tp01_rows'
    sink-table: ${FUNC_SNK_DB}.tp01_rows_out
  - source-table: '${FUNC_SRC_DB}\.tp02_keys'
    sink-table: ${FUNC_SNK_DB}.tp02_keys_out
  - source-table: '${FUNC_SRC_DB}\.tp09_add'
    sink-table: ${FUNC_SNK_DB}.tp09_add_out
  - source-table: '${FUNC_SRC_DB}\.tp10_ddl'
    sink-table: ${FUNC_SNK_DB}.tp10_ddl_out
  - source-table: '${FUNC_SRC_DB}\.tp07_fail'
    sink-table: no_such_db.tp07_fail_out

transform:
  - source-table: '${FUNC_SRC_DB}\.tp01_rows'
    projection: id, name, city
    filter: city = 'hz'
    primary-keys: id
  - source-table: '${FUNC_SRC_DB}\.tp02_keys'
    projection: docid, title
    primary-keys: docid
  - source-table: '${FUNC_SRC_DB}\.tp07_fail'
    projection: id, name
    primary-keys: id
  - source-table: '${FUNC_SRC_DB}\.tp09_add'
    projection: id, name
    primary-keys: id
  - source-table: '${FUNC_SRC_DB}\.tp10_ddl'
    projection: id, name
    primary-keys: id

pipeline:
  name: ${PIPELINE_NAME}
  parallelism: 1

checkpoint:
  storage: file
  interval: 2s
  file-path: ${CHECKPOINT_DIR}
