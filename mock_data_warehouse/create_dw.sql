DROP DATABASE IF EXISTS mock_dw;
CREATE DATABASE mock_dw;

\c mock_dw

DROP TABLE IF EXISTS dim_currency;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
    date_id DATE PRIMARY KEY NOT NULL,
    year INT4 NOT NULL,
    month INT4 NOT NULL,
    day INT4 NOT NULL,
    day_of_week INT4 NOT NULL,
    day_name VARCHAR NOT NULL,
    month_name VARCHAR NOT NULL,
    quarter INT4 NOT NULL
);

CREATE TABLE dim_currency (
    currency_id SERIAL PRIMARY KEY NOT NULL,
    currency_code VARCHAR NOT NULL,
    currency_name VARCHAR NOT NULL
);

INSERT INTO dim_date
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
