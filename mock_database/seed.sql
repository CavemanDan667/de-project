DROP DATABASE IF EXISTS mock_tote_db;
CREATE DATABASE mock_tote_db;

\c mock_tote_db;



CREATE TABLE table_1 (
    column_1 INT,
    column_2 VARCHAR(50),
    column_3 INT
);

INSERT INTO table_1 (column_1, column_2, column_3)
VALUES (1, 'one', 10),
        (2, 'two', 20),
        (3, 'three', 30);

CREATE TABLE table_2 (
    column_a INT,
    column_b VARCHAR(50),
    column_c INT
);

INSERT INTO table_2 (column_a, column_b, column_c)
VALUES (1, 'one', 10),
        (2, 'two', 20),
        (3, 'three', 30);

CREATE TABLE _table_3 (
    column_private INT
);