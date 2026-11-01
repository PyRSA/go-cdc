source:
  type: mysql
  hostname: ${MYSQL_HOST}
  port: ${MYSQL_PORT}
  username: ${SRC_USER}
  password: "${SRC_PASSWORD}"
  tables: '${FUNC_SRC_DB}\.tpk01_.*'
  server-id: ${CDC_SERVER_ID_KAFKA}
  server-time-zone: Asia/Shanghai
  scan.startup.mode: initial
  schema-change.enabled: true
  connect.timeout: 30s

sink:
  type: kafka
  brokers: ${KAFKA_BROKERS}
  topic: ${KAFKA_DEFAULT_TOPIC}
  source-name: ${PIPELINE_KAFKA_NAME}
  schema.change.emit.enabled: ${KAFKA_EMIT_DDL}

route:
  - source-table: '${FUNC_SRC_DB}\.tpk01_users'
    sink-table: kafka.tpk01_users
  - source-table: '${FUNC_SRC_DB}\.tpk01_orders'
    sink-table: kafka.tpk01_orders
  # tpk01_misc has no route → default topic

transform:
  - source-table: '${FUNC_SRC_DB}\.tpk01_users'
    projection: id, name, city
    filter: city = 'hz'
    primary-keys: id
  - source-table: '${FUNC_SRC_DB}\.tpk01_orders'
    projection: id, user_id, amt
    primary-keys: id
  - source-table: '${FUNC_SRC_DB}\.tpk01_misc'
    projection: id, note
    primary-keys: id

pipeline:
  name: ${PIPELINE_KAFKA_NAME}
  parallelism: 1

checkpoint:
  storage: file
  interval: 2s
  file-path: ${CHECKPOINT_KAFKA_DIR}
