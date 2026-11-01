-- Doris pipeline suite seed (TP-DR01..DR04). Tables under db_tp_func_src.
USE db_tp_func_src;

DROP TABLE IF EXISTS tpds01_users;
DROP TABLE IF EXISTS tpds01_orders;
DROP TABLE IF EXISTS tpds01_misc;
DROP TABLE IF EXISTS tpds01_evolve;

CREATE TABLE tpds01_users (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  city VARCHAR(32) NOT NULL,
  secret VARCHAR(32) NOT NULL DEFAULT 'x'
) ENGINE=InnoDB;

CREATE TABLE tpds01_orders (
  id BIGINT PRIMARY KEY,
  user_id BIGINT NOT NULL,
  amt DECIMAL(10,2) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE tpds01_misc (
  id BIGINT PRIMARY KEY,
  note VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

-- No transform projection: used for ADD COLUMN (TP-DR03) so new cols reach the sink.
CREATE TABLE tpds01_evolve (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

INSERT INTO tpds01_users (id, name, city, secret) VALUES
  (1, 'alice', 'hz', 's1'),
  (2, 'bob', 'sh', 's2');

INSERT INTO tpds01_orders (id, user_id, amt) VALUES
  (10, 1, 9.50),
  (11, 2, 3.00);

INSERT INTO tpds01_misc (id, note) VALUES
  (100, 'default-route');

INSERT INTO tpds01_evolve (id, name) VALUES
  (1, 'base');
