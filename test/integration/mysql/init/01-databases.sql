-- Anonymous IT principals + suite databases (no production-like names).
-- Users look random; databases are named by test suite.

CREATE DATABASE IF NOT EXISTS db_tp_func_src DEFAULT CHARACTER SET utf8mb4;
CREATE DATABASE IF NOT EXISTS db_tp_func_snk DEFAULT CHARACTER SET utf8mb4;
CREATE DATABASE IF NOT EXISTS db_tp_types_src DEFAULT CHARACTER SET utf8mb4;
CREATE DATABASE IF NOT EXISTS db_tp_types_snk DEFAULT CHARACTER SET utf8mb4;
CREATE DATABASE IF NOT EXISTS db_tp_load_src DEFAULT CHARACTER SET utf8mb4;
CREATE DATABASE IF NOT EXISTS db_tp_load_snk DEFAULT CHARACTER SET utf8mb4;

CREATE USER IF NOT EXISTS 'u_a7k9m2x4'@'%' IDENTIFIED BY 'P_w8q3n5v1r6';
CREATE USER IF NOT EXISTS 'u_b3j6c8y1'@'%' IDENTIFIED BY 'P_z2h5t9k4m7';

GRANT ALL PRIVILEGES ON db_tp_func_src.* TO 'u_a7k9m2x4'@'%';
GRANT ALL PRIVILEGES ON db_tp_types_src.* TO 'u_a7k9m2x4'@'%';
GRANT ALL PRIVILEGES ON db_tp_load_src.* TO 'u_a7k9m2x4'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD, PROCESS ON *.* TO 'u_a7k9m2x4'@'%';

GRANT ALL PRIVILEGES ON db_tp_func_snk.* TO 'u_b3j6c8y1'@'%';
GRANT ALL PRIVILEGES ON db_tp_types_snk.* TO 'u_b3j6c8y1'@'%';
GRANT ALL PRIVILEGES ON db_tp_load_snk.* TO 'u_b3j6c8y1'@'%';
-- sink may try CREATE DATABASE for failed-route case; intentionally no grant on no_such_db

FLUSH PRIVILEGES;
