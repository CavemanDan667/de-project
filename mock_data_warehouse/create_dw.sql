DROP DATABASE IF EXISTS mock_dw;
CREATE DATABASE mock_dw;

\c mock_dw

DROP TABLE IF EXISTS fact_payment;
DROP TABLE IF EXISTS dim_transaction;
DROP TABLE IF EXISTS fact_sales_order;
DROP TABLE IF EXISTS fact_purchase_order;
DROP TABLE IF EXISTS dim_counterparty;
DROP TABLE IF EXISTS dim_design;
DROP TABLE IF EXISTS dim_payment_type;
DROP TABLE IF EXISTS dim_location;
DROP TABLE IF EXISTS dim_staff;
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
    currency_id INT PRIMARY KEY NOT NULL,
    currency_code VARCHAR NOT NULL,
    currency_name VARCHAR NOT NULL
);

CREATE TABLE dim_staff (
    staff_id INT RIMARY KEY NOT NULL,
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    department_name VARCHAR NOT NULL,
    location VARCHAR NOT NULL,
    email_address VARCHAR NOT NULL
);

CREATE TABLE dim_location (
    location_id INT PRIMARY KEY NOT NULL,
    address_line_1 VARCHAR NOT NULL,
    address_line_2 VARCHAR,
    district VARCHAR,
    city VARCHAR NOT NULL,
    postal_code VARCHAR NOT NULL,
    country VARCHAR NOT NULL,
    phone VARCHAR NOT NULL
);

CREATE TABLE dim_payment_type (
    payment_type_id INT PRIMARY KEY NOT NULL,
    payment_type_name VARCHAR NOT NULL
);

CREATE TABLE dim_design (
    design_id INT PRIMARY KEY NOT NULL,
    design_name VARCHAR NOT NULL,
    file_location VARCHAR NOT NULL,
    file_name VARCHAR NOT NULL
);

CREATE TABLE dim_counterparty (
    counterparty_id INT PRIMARY KEY NOT NULL,
    counterparty_legal_name VARCHAR NOT NULL,
    counterparty_legal_address_line_1 VARCHAR NOT NULL,
    counterparty_legal_address_line_2 VARCHAR,
    counterparty_legal_district VARCHAR,
    counterparty_legal_city VARCHAR NOT NULL,
    counterparty_legal_postal_code VARCHAR NOT NULL,
    counterparty_legal_country VARCHAR NOT NULL,
    counterparty_legal_phone_number VARCHAR NOT NULL
);

CREATE TABLE fact_sales_order (
    sales_record_id SERIAL PRIMARY KEY NOT NULL,
    sales_order_id INT NOT NULL,
    created_date DATE NOT NULL REFERENCES dim_date(date_id),
    created_time TIME NOT NULL,
    last_updated_date DATE NOT NULL REFERENCES dim_date(date_id),
    last_updated_time TIME NOT NULL,
    sales_staff_id INT NOT NULL REFERENCES dim_staff(staff_id),
    counterparty_id INT NOT NULL REFERENCES dim_counterparty(counterparty_id),
    units_sold INT NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL,
    currency_id INT NOT NULL REFERENCES dim_currency(currency_id),
    design_id INT NOT NULL REFERENCES dim_design(design_id),
    agreed_payment_date DATE NOT NULL REFERENCES dim_date(date_id),
    agreed_delivery_date DATE NOT NULL REFERENCES dim_date(date_id),
    agreed_delivery_location_id INT NOT NULL REFERENCES dim_location(location_id)
);

CREATE TABLE fact_purchase_order (
    purchase_record_id SERIAL PRIMARY KEY NOT NULL,
    purchase_order_id INT NOT NULL,
    created_date DATE NOT NULL REFERENCES dim_date(date_id),
    created_time TIME NOT NULL,
    last_updated_date DATE NOT NULL REFERENCES dim_date(date_id),
    last_updated_time TIME NOT NULL,
    staff_id INT NOT NULL REFERENCES dim_staff(staff_id),
    counterparty_id INT NOT NULL REFERENCES dim_counterparty(counterparty_id),
    item_code VARCHAR NOT NULL,
    item_quantity INT NOT NULL,
    item_unit_price NUMERIC NOT NULL,
    currency_id INT NOT NULL REFERENCES dim_currency(currency_id),
    agreed_delivery_date DATE NOT NULL REFERENCES dim_date(date_id),
    agreed_payment_date DATE NOT NULL REFERENCES dim_date(date_id),
    agreed_delivery_location_id INT NOT NULL REFERENCES dim_location(location_id)
);

CREATE TABLE dim_transaction (
    transaction_id INT PRIMARY KEY NOT NULL,
    transaction_type VARCHAR NOT NULL,
    sales_order_id INT REFERENCES fact_sales_order(sales_record_id),
    purchase_order_id INT REFERENCES fact_purchase_order(purchase_record_id)
);

CREATE TABLE fact_payment (
    payment_record_id SERIAL PRIMARY KEY NOT NULL,
    payment_id INT NOT NULL,
    created_date DATE NOT NULL REFERENCES dim_date(date_id),
    created_time TIME NOT NULL,
    last_updated_date DATE NOT NULL REFERENCES dim_date(date_id),
    last_updated_time TIME NOT NULL,
    transaction_id INT NOT NULL REFERENCES dim_transaction(transaction_id),
    counterparty_id INT NOT NULL REFERENCES dim_counterparty(counterparty_id),
    payment_amount NUMERIC NOT NULL,
    currency_id INT NOT NULL REFERENCES dim_currency(currency_id),
    payment_type_id INT NOT NULL REFERENCES dim_payment_type(payment_type_id),
    paid BOOLEAN NOT NULL,
    payment_date DATE REFERENCES dim_date(date_id)
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
