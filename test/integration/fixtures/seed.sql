-- Functional suite seed (TP-01..TP-12). Table names follow test-point ids.
USE db_tp_func_src;

DROP TABLE IF EXISTS tp10_ddl;
DROP TABLE IF EXISTS tp01_rows;
DROP TABLE IF EXISTS tp02_keys;
DROP TABLE IF EXISTS tp09_add;
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

CREATE TABLE tp09_add (
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
INSERT INTO tp09_add(id, name) VALUES (7, 'add-row');
-- tp07_fail stays empty: create to no_such_db soft-fails; a later INSERT asserts write fails the job.
