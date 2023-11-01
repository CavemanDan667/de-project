DROP DATABASE IF EXISTS mock_tote_db;
CREATE DATABASE mock_tote_db;

\c mock_tote_db;



CREATE TABLE table_1 (
    column_1 INT,
    column_2 VARCHAR(50),
    column_3 INT,
    last_updated TIMESTAMP
);

INSERT INTO table_1 (column_1, column_2, column_3, last_updated)
VALUES (1, 'one', 10, '2020-01-01 00:00:00'),
        (2, 'two', 20, '2020-01-01 00:00:00'),
        (3, 'three', 30, '2023-11-01 00:00:00');

CREATE TABLE table_2 (
    column_a INT,
    column_b VARCHAR(50),
    column_c INT,
    last_updated TIMESTAMP
);

CREATE TABLE _table_3 (
);

SELECT * FROM table_1 WHERE last_updated BETWEEN '2022-01-01 00:00:00' AND '2024-01-01 00:00:00';
