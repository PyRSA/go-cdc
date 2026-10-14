-- Functional suite seed (TP-01..TP-20). Table names follow test-point ids.
USE db_tp_func_src;

DROP TABLE IF EXISTS tp11_ddl;
DROP TABLE IF EXISTS tp15_crt;
DROP TABLE IF EXISTS tp16_crt;
DROP TABLE IF EXISTS tp17_crt;
DROP TABLE IF EXISTS tp18_crt;
DROP TABLE IF EXISTS tp19_crt;
DROP TABLE IF EXISTS tp20_crt;
DROP TABLE IF EXISTS tp01_rows;
DROP TABLE IF EXISTS tp02_keys;
DROP TABLE IF EXISTS tp10_add;
DROP TABLE IF EXISTS tp15_diff;
DROP TABLE IF EXISTS tp16_diff;
DROP TABLE IF EXISTS tp17_diff;
DROP TABLE IF EXISTS tp18_diff;
DROP TABLE IF EXISTS tp19_diff;
DROP TABLE IF EXISTS tp20_diff;
DROP TABLE IF EXISTS tp07_fail;

CREATE TABLE tp01_rows (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  city VARCHAR(32) NOT NULL,
  amount DECIMAL(10,2) NULL
) ENGINE=InnoDB;

CREATE TABLE tp02_keys (
  id BIGINT PRIMARY KEY,
  docid VARCHAR(64) NOT NULL,
  title VARCHAR(128) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE tp10_add (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE tp15_diff (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE tp16_diff (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE tp17_diff (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE tp18_diff (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE tp19_diff (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE tp20_diff (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE tp07_fail (
  id BIGINT PRIMARY KEY,
  name VARCHAR(64) NOT NULL
) ENGINE=InnoDB;

INSERT INTO tp01_rows(id, name, city, amount) VALUES
  (1, 'alice', 'hz', 10.50),
  (2, 'bob', 'sh', 3.00),
  (3, 'ann', 'hz', NULL);

INSERT INTO tp02_keys(id, docid, title) VALUES (1, 'A', 'title-a');
INSERT INTO tp10_add(id, name) VALUES (7, 'add-row');
INSERT INTO tp15_diff(id, name) VALUES (15, 'seed-15');
INSERT INTO tp16_diff(id, name) VALUES (16, 'seed-16');
INSERT INTO tp17_diff(id, name) VALUES (17, 'seed-17');
INSERT INTO tp18_diff(id, name) VALUES (18, 'seed-18');
INSERT INTO tp19_diff(id, name) VALUES (19, 'seed-19');
INSERT INTO tp20_diff(id, name) VALUES (20, 'seed-20');
-- tp07_fail stays empty: create to no_such_db soft-fails; a later INSERT asserts write fails the job.
