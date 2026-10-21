-- Kafka functional suite seed (TP-K01..K04). Tables under db_tp_func_src.
USE db_tp_func_src;

DROP TABLE IF EXISTS tpk01_users;
DROP TABLE IF EXISTS tpk01_orders;
DROP TABLE IF EXISTS tpk01_misc;

CREATE TABLE tpk01_users (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  city VARCHAR(32) NOT NULL,
  secret VARCHAR(32) NOT NULL DEFAULT 'x'
) ENGINE=InnoDB;

CREATE TABLE tpk01_orders (
  id BIGINT PRIMARY KEY,
  user_id BIGINT NOT NULL,
  amt DECIMAL(10,2) NOT NULL
) ENGINE=InnoDB;

-- No route rule → must land on sink.topic (default).
CREATE TABLE tpk01_misc (
  id BIGINT PRIMARY KEY,
  note VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

INSERT INTO tpk01_users (id, name, city, secret) VALUES
  (1, 'alice', 'hz', 's1'),
  (2, 'bob', 'sh', 's2');

INSERT INTO tpk01_orders (id, user_id, amt) VALUES
  (10, 1, 9.50),
  (11, 2, 3.00);

INSERT INTO tpk01_misc (id, note) VALUES
  (100, 'default-topic');
