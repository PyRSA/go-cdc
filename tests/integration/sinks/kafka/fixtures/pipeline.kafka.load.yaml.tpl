source:
  type: mysql
  hostname: ${MYSQL_HOST}
  port: ${MYSQL_PORT}
  username: ${SRC_USER}
  password: "${SRC_PASSWORD}"
  tables: '${LOAD_SRC_DB}\.tpl01_(t1|typed|large)'
  server-id: ${CDC_SERVER_ID_KAFKA_LOAD}
  server-time-zone: Asia/Shanghai
  scan.startup.mode: initial
  scan.incremental.snapshot.chunk.size: 4096
  scan.snapshot.fetch.size: 1024
  schema-change.enabled: false
  connect.timeout: 30s

sink:
  type: kafka
  brokers: ${KAFKA_BROKERS}
  topic: ${KAFKA_DEFAULT_TOPIC}
  source-name: ${PIPELINE_KAFKA_LOAD_NAME}
  schema.change.emit.enabled: false
  write-batch-size: 1000
  write-batch-interval: 500ms

route:
  - source-table: '${LOAD_SRC_DB}\.tpl01_t1'
    sink-table: kafka.tpl01_t1
  - source-table: '${LOAD_SRC_DB}\.tpl01_typed'
    sink-table: kafka.tpl01_typed
  - source-table: '${LOAD_SRC_DB}\.tpl01_large'
    sink-table: kafka.tpl01_large

transform:
  - source-table: '${LOAD_SRC_DB}\.tpl01_.*'
    projection: '*'
    primary-keys: id

pipeline:
  name: ${PIPELINE_KAFKA_LOAD_NAME}
  parallelism: ${LOAD_PARALLELISM}

checkpoint:
  storage: file
  interval: 3s
  file-path: ${CHECKPOINT_KAFKA_LOAD_DIR}
