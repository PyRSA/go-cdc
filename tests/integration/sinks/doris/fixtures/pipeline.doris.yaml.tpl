source:
  type: mysql
  hostname: ${MYSQL_HOST}
  port: ${MYSQL_PORT}
  username: ${SRC_USER}
  password: "${SRC_PASSWORD}"
  tables: '${FUNC_SRC_DB}\.tpds01_.*'
  server-id: ${CDC_SERVER_ID_DORIS}
  server-time-zone: Asia/Shanghai
  scan.startup.mode: initial
  schema-change.enabled: true
  connect.timeout: 30s

sink:
  type: doris
  hostname: ${DORIS_HOST}
  port: ${DORIS_HTTP_PORT}
  sql-port: ${DORIS_SQL_PORT}
  username: ${DORIS_USER}
  password: "${DORIS_PASSWORD}"
  auto-create-table: true
  replication-num: ${DORIS_REPLICATION_NUM}
  schema.change.enabled: true

route:
  - source-table: '${FUNC_SRC_DB}\.tpds01_users'
    sink-table: ${DORIS_SNK_DB}.tpds01_users
  - source-table: '${FUNC_SRC_DB}\.tpds01_orders'
    sink-table: ${DORIS_SNK_DB}.tpds01_orders
  - source-table: '${FUNC_SRC_DB}\.tpds01_misc'
    sink-table: ${DORIS_SNK_DB}.tpds01_misc
  - source-table: '${FUNC_SRC_DB}\.tpds01_evolve'
    sink-table: ${DORIS_SNK_DB}.tpds01_evolve

transform:
  - source-table: '${FUNC_SRC_DB}\.tpds01_users'
    projection: id, name, city
    filter: city = 'hz'
    primary-keys: id
  - source-table: '${FUNC_SRC_DB}\.tpds01_orders'
    projection: id, user_id, amt
    primary-keys: id
  - source-table: '${FUNC_SRC_DB}\.tpds01_misc'
    projection: id, note
    primary-keys: id
  - source-table: '${FUNC_SRC_DB}\.tpds01_evolve'
    primary-keys: id

pipeline:
  name: ${PIPELINE_DORIS_NAME}
  parallelism: 1

checkpoint:
  storage: file
  interval: 2s
  file-path: ${CHECKPOINT_DORIS_DIR}
