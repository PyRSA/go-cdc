source:
  type: mysql
  hostname: ${MYSQL_HOST}
  port: ${MYSQL_PORT}
  username: ${SRC_USER}
  password: "${SRC_PASSWORD}"
  tables: '${FUNC_SRC_DB}\.tp01_rows'
  server-id: ${CDC_SERVER_ID_STDOUT}
  server-time-zone: Asia/Shanghai
  scan.startup.mode: initial
  schema-change.enabled: true
  connect.timeout: 30s

sink:
  type: stdout

route:
  - source-table: '${FUNC_SRC_DB}\.tp01_rows'
    sink-table: ${FUNC_SRC_DB}.tp01_rows

transform:
  - source-table: '${FUNC_SRC_DB}\.tp01_rows'
    projection: id, name, city
    filter: city = 'hz'
    primary-keys: id

pipeline:
  name: ${PIPELINE_STDOUT_NAME}
  parallelism: 1

checkpoint:
  storage: file
  interval: 2s
  file-path: ${CHECKPOINT_STDOUT_DIR}
