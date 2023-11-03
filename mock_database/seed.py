from pg8000.native import Connection

def test_db_seeder():
    conn = Connection(user="mock_tote_db",
                      host="localhost",
                      password="postgres")
    conn.run("DROP TABLE IF EXISTS table_3;")
    conn.run("DROP TABLE IF EXISTS table_2;")
    conn.run("DROP TABLE IF EXISTS table_1;")
    conn.run("CREATE TABLE table_1 (column_1 INT, column_2 VARCHAR(50), column_3 INT, last_updated TIMESTAMP);")
    conn.run("INSERT INTO table_1 (column_1, column_2, column_3, last_updated) VALUES (1, 'one', 10, '2020-01-01 00:00:00'), (2, 'two', 20, '2020-01-01 00:00:00'), (3, 'three', 30, '2023-11-01 00:00:00');")
    conn.run("CREATE TABLE table_2 (column_a INT, column_b VARCHAR(50), column_c INT, last_updated TIMESTAMP);")
    conn.run("CREATE TABLE _table_3 ();")
    conn.run("SELECT * FROM table_1 WHERE last_updated BETWEEN '2022-01-01 00:00:00' AND '2024-01-01 00:00:00';")
    conn.run("CREATE DATABASE mock_empty_db")
    
test_db_seeder()