source:
  type: mysql
  hostname: ${MYSQL_HOST}
  port: ${MYSQL_PORT}
  username: ${SRC_USER}
  password: "${SRC_PASSWORD}"
  tables: '${LOAD_SRC_DB}\.tpdl01_.*'
  server-id: ${CDC_SERVER_ID_DORIS_LOAD}
  server-time-zone: Asia/Shanghai
  scan.startup.mode: initial
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

route:
  - source-table: '${LOAD_SRC_DB}\.tpdl01_t1'
    sink-table: ${DORIS_SNK_DB}.tpdl01_t1
  - source-table: '${LOAD_SRC_DB}\.tpdl01_t2'
    sink-table: ${DORIS_SNK_DB}.tpdl01_t2
  - source-table: '${LOAD_SRC_DB}\.tpdl01_t3'
    sink-table: ${DORIS_SNK_DB}.tpdl01_t3
  - source-table: '${LOAD_SRC_DB}\.tpdl01_t4'
    sink-table: ${DORIS_SNK_DB}.tpdl01_t4
  - source-table: '${LOAD_SRC_DB}\.tpdl01_typed'
    sink-table: ${DORIS_SNK_DB}.tpdl01_typed

# Every load table is keyed by id; Doris auto-create needs the key list to emit
# a UNIQUE KEY table. No projection: bulk volume must exercise every column.
transform:
  - source-table: '${LOAD_SRC_DB}\.tpdl01_t1'
    primary-keys: id
  - source-table: '${LOAD_SRC_DB}\.tpdl01_t2'
    primary-keys: id
  - source-table: '${LOAD_SRC_DB}\.tpdl01_t3'
    primary-keys: id
  - source-table: '${LOAD_SRC_DB}\.tpdl01_t4'
    primary-keys: id
  - source-table: '${LOAD_SRC_DB}\.tpdl01_typed'
    primary-keys: id

pipeline:
  name: ${PIPELINE_DORIS_LOAD_NAME}
  parallelism: ${LOAD_PARALLELISM}

checkpoint:
  storage: file
  interval: 2s
  file-path: ${CHECKPOINT_DORIS_LOAD_DIR}
