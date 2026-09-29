-- Comprehensive MySQL type coverage for go-cdc IT.
-- Seed under Asia/Shanghai (+08:00) so TIMESTAMP wall-clock is deterministic.
USE db_tp_types_src;

SET time_zone = '+08:00';

DROP TABLE IF EXISTS tpd01_types;

CREATE TABLE tpd01_types (
  id            BIGINT PRIMARY KEY,
  -- integers
  c_tinyint     TINYINT           NOT NULL,
  c_tinyint_u   TINYINT UNSIGNED  NOT NULL,
  c_smallint    SMALLINT          NOT NULL,
  c_int         INT               NOT NULL,
  c_bigint      BIGINT            NOT NULL,
  c_bigint_u    BIGINT UNSIGNED   NOT NULL,
  -- decimal / float
  c_decimal     DECIMAL(10,4)     NOT NULL,
  c_numeric     NUMERIC(18,6)     NOT NULL,
  c_float       FLOAT             NOT NULL,
  c_double      DOUBLE            NOT NULL,
  -- strings
  c_char        CHAR(8)           NOT NULL,
  c_varchar     VARCHAR(64)       NOT NULL,
  c_text        TEXT              NOT NULL,
  -- temporal (timezone-sensitive)
  c_date        DATE              NOT NULL,
  c_time        TIME(3)           NOT NULL,
  c_datetime    DATETIME(6)       NOT NULL,
  c_timestamp   TIMESTAMP(6)      NULL,
  -- misc
  c_json        JSON              NULL,
  c_enum        ENUM('red','green','blue') NOT NULL,
  c_set         SET('x','y','z')  NOT NULL,
  c_bit         BIT(8)            NOT NULL,
  c_bool        TINYINT(1)        NOT NULL,
  c_blob        BLOB              NULL,
  c_binary      BINARY(4)         NOT NULL,
  c_null_int    INT               NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- id=1: snapshot baseline (all representative values)
INSERT INTO tpd01_types (
  id,
  c_tinyint, c_tinyint_u, c_smallint, c_int, c_bigint, c_bigint_u,
  c_decimal, c_numeric, c_float, c_double,
  c_char, c_varchar, c_text,
  c_date, c_time, c_datetime, c_timestamp,
  c_json, c_enum, c_set, c_bit, c_bool, c_blob, c_binary, c_null_int
) VALUES (
  1,
  -12, 200, -30000, 123456789, -9007199254740991, 18446744073709551615,
  1234.5678, 9876543210.123456, 3.1415, 2.718281828459,
  'abcdefgh', 'hello-world', 'text-line-1\ntext-line-2',
  '2024-03-15', '13:45:30.123', '2024-03-15 13:45:30.123456', '2024-03-15 13:45:30.123456',
  JSON_OBJECT('k', 'v', 'n', 1),
  'green',
  'x,z',
  b'10101010',
  1,
  X'DEADBEEF',
  X'01020304',
  NULL
);

-- id=2: NULL / empty-ish edges for nullable columns (temporal NULL on timestamp)
INSERT INTO tpd01_types (
  id,
  c_tinyint, c_tinyint_u, c_smallint, c_int, c_bigint, c_bigint_u,
  c_decimal, c_numeric, c_float, c_double,
  c_char, c_varchar, c_text,
  c_date, c_time, c_datetime, c_timestamp,
  c_json, c_enum, c_set, c_bit, c_bool, c_blob, c_binary, c_null_int
) VALUES (
  2,
  0, 0, 0, 0, 0, 0,
  0.0000, 0.000000, 0, 0,
  '', '', '',
  '1970-01-01', '00:00:00.000', '1970-01-01 00:00:00.000000', NULL,
  NULL,
  'red',
  'y',
  b'00000000',
  0,
  NULL,
  X'00000000',
  NULL
);
