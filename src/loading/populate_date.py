from pg8000.native import Connection
from dotenv import dotenv_values

config = dotenv_values('.env')

user = config["DW_USER"]
password = config["DW_PASSWORD"]
host = config["DW_HOST"]
port = config["DW_PORT"]
database = config["DW_DATABASE"]

conn = Connection(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database)

conn.run('''INSERT INTO dim_date
SELECT datum AS date_id,
    EXTRACT(YEAR FROM datum) AS year,
    EXTRACT(MONTH FROM datum) AS month,
    EXTRACT(DAY FROM datum) AS day,
    EXTRACT(ISODOW FROM datum) AS day_of_week,
    TO_CHAR(datum,'TMDay') AS day_name,
    TO_CHAR(datum, 'TMMonth') AS month_name,
    EXTRACT(QUARTER FROM datum) AS quarter
FROM (SELECT generate_series(
    DATE('2020-01-01'),
    DATE('2029-12-31'),
    '1 day'::interval)::DATE as datum)
    AS date_sequence;
''')
