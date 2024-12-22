-- Altering data types for the orders table
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR(50),
ALTER COLUMN store_code TYPE VARCHAR(50),
ALTER COLUMN product_code TYPE VARCHAR(50),
ALTER COLUMN product_quantity TYPE SMALLINT USING product_quantity::SMALLINT;

-- Altering data types for the dim_users table
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN join_date TYPE DATE USING join_date::DATE,
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
ALTER COLUMN country_code TYPE VARCHAR(3) USING country_code::VARCHAR(3);

-- Altering data types for the dim_store_details table
ALTER TABLE dim_store_details
DROP COLUMN lat;

UPDATE dim_store_details
SET latitude = NULL
WHERE latitude = 'N/A';

UPDATE dim_store_details
SET address = NULL
WHERE address = 'N/A';

UPDATE dim_store_details
SET longitude = NULL
WHERE longitude = 'N/A';

UPDATE dim_store_details
SET locality = NULL
WHERE locality = 'N/A';

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE NUMERIC USING longitude::NUMERIC,
ALTER COLUMN latitude TYPE NUMERIC USING latitude::NUMERIC,
ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
ALTER COLUMN continent TYPE VARCHAR(255),
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN store_type DROP NOT NULL,
ALTER COLUMN country_code TYPE VARCHAR(2) USING country_code::VARCHAR(2),
ALTER COLUMN store_code TYPE VARCHAR(12) USING store_code::VARCHAR(12);

-- Altering the data types for the dim_products table
ALTER TABLE dim_products ADD COLUMN weight_class VARCHAR(15);

UPDATE dim_products
SET weight_class = CASE
    WHEN weight < 2 THEN 'Light'
	WHEN weight BETWEEN 2 AND 39.99 THEN 'Mid_Sized'
	WHEN weight BETWEEN 40 AND 139.99 THEN 'Heavy'
	WHEN weight >= 140 THEN 'Truck_Required'
	ELSE 'Unknown'
END;

UPDATE dim_products
SET product_price = REPLACE(product_price, '£', '');

ALTER TABLE dim_products RENAME COLUMN removed TO still_available;

ALTER TABLE dim_products 
ALTER COLUMN product_price TYPE NUMERIC USING product_price::NUMERIC,
ALTER COLUMN weight TYPE NUMERIC USING weight::NUMERIC,
ALTER COLUMN date_added TYPE DATE USING date_added::DATE;

UPDATE dim_products
SET still_available = CASE
    WHEN LOWER(still_available) IN ('still_available') THEN 'TRUE'
    WHEN LOWER(still_available) IN ('removed') THEN 'FALSE'

END;

UPDATE dim_products
SET still_available = TRUE
WHERE still_available IS NULL;

ALTER TABLE dim_products
ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN "EAN" TYPE VARCHAR(17);

-- Altering the data types for the dim_date_times table
ALTER TABLE dim_date_times
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN "month" TYPE VARCHAR(2),
ALTER COLUMN "year" TYPE VARCHAR(4),
ALTER COLUMN "day" TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10);

-- Altering the date types for the dim_card_details table
ALTER TABLE dim_date_times
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN card_number TYPE VARCHAR(22),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;

UPDATE dim_card_details
SET card_number = REPLACE(card_number, '?', '');


-- Adding primary keys to the dimension tables
ALTER TABLE dim_users
ADD CONSTRAINT pk_user_uuid PRIMARY KEY (user_uuid);

ALTER TABLE dim_date_times
ADD CONSTRAINT pk_date_uuid PRIMARY KEY (date_uuid);

ALTER TABLE dim_card_details
ADD CONSTRAINT pk_card_number PRIMARY KEY (card_number);

ALTER TABLE dim_store_details
ADD CONSTRAINT pk_store_code PRIMARY KEY (store_code);

ALTER TABLE dim_products
ADD CONSTRAINT pk_product_code PRIMARY KEY (product_code);

-- Adding foreign keys
ALTER TABLE orders_table
ADD CONSTRAINT fk_user
FOREIGN KEY (user_uuid)
REFERENCES dim_users (user_uuid)
ON DELETE CASCADE
ON UPDATE CASCADE;

ALTER TABLE orders_table
ADD CONSTRAINT fk_store
FOREIGN KEY (store_code)
REFERENCES dim_store_details (store_code)
ON DELETE CASCADE
ON UPDATE CASCADE;

ALTER TABLE orders_table
ADD CONSTRAINT fk_products
FOREIGN KEY (product_code)
REFERENCES dim_products (product_code)
ON DELETE CASCADE
ON UPDATE CASCADE;

ALTER TABLE orders_table
ADD CONSTRAINT fk_date
FOREIGN KEY (date_uuid)
REFERENCES dim_date_times (date_uuid)
ON DELETE CASCADE
ON UPDATE CASCADE;

ALTER TABLE orders_table
ADD CONSTRAINT fk_card
FOREIGN KEY (card_number)
REFERENCES dim_card_details (card_number)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- Querying the Data

-- Task 1

SELECT 
    country_code AS country,
    COUNT(DISTINCT store_code) AS total_no_stores
FROM 
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_no_stores DESC;

(Return 266 entries for GB webportal still GB)


-- Task 2

SELECT
    locality,
    COUNT(locality) AS total_no_stores
FROM 
    dim_store_details
GROUP BY
    locality
ORDER BY
    total_no_stores DESC
LIMIT 7;
	

-- Task 3


SELECT 
    SUM(orders.product_quantity * products.product_price) AS total_sales,
	datetime.month
FROM 
    orders_table orders
JOIN 
    dim_date_times datetime ON orders.date_uuid = datetime.date_uuid
JOIN 
    dim_products products ON orders.product_code = products.product_code
GROUP BY 
    datetime.month
ORDER BY 
    total_sales DESC
LIMIT 6;


-- Task 4


SELECT
	COUNT(orders.product_code) AS number_of_sales,
	SUM(orders.product_quantity) AS product_quantity_count,
	CASE 
        WHEN store.store_type = 'Web Portal' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM 
	orders_table orders
JOIN
	dim_store_details store ON store.store_code = orders.store_code
GROUP BY 
	location
ORDER BY
	number_of_sales;
	

-- Task 5


SELECT 
	store.store_type,
	SUM(orders.product_quantity * products.product_price) AS total_sales,
	ROUND(
        (SUM(orders.product_quantity * products.product_price) * 100.0) 
        / SUM(SUM(orders.product_quantity * products.product_price)) OVER (), 
        2
    ) AS "sales_made(%)"
FROM
	orders_table orders
JOIN 
	dim_store_details store ON orders.store_code = store.store_code
JOIN 
	dim_products products ON orders.product_code = products.product_code
GROUP BY 
	store.store_type
ORDER BY
	total_sales DESC;

(MAYBE REVISIT SLIGHT VARIATIONS IN PERCENTAGE BUT STILL TOTAL 100)


-- Task 6


SELECT
	SUM(orders.product_quantity * products.product_price) AS total_sales,
	year,
	month
FROM
	orders_table orders
JOIN 
	dim_date_times date ON date.date_uuid = orders.date_uuid
JOIN
	dim_products products ON products.product_code = orders.product_code
GROUP BY
	date.year,
	date.month
ORDER BY
	total_sales DESC
LIMIT 
	10;


-- Task 7


SELECT
	SUM(staff_numbers) AS total_staff_numbers,
	country_code
FROM
	dim_store_details
GROUP BY
	country_code
ORDER BY 
	total_staff_numbers DESC;


-- Task 8


SELECT
	SUM(products.product_price * orders.product_quantity) AS total_sales,
	store.store_type,
	store.country_code
FROM
	orders_table orders
JOIN
	dim_store_details store ON store.store_code = orders.store_code
JOIN
	dim_products products ON products.product_code = orders.product_code
WHERE
	store.country_code = 'DE'
GROUP BY
	store.store_type,
	store.country_code
ORDER BY
	total_sales;
	

-- Task 9

WITH ordered_timestamps AS (
    SELECT 
        year::INT AS year,
        MAKE_TIMESTAMP(
            year::INT, 
            month::INT, 
            day::INT, 
            EXTRACT(HOUR FROM timestamp::TIME)::INT, 
            EXTRACT(MINUTE FROM timestamp::TIME)::INT, 
            EXTRACT(SECOND FROM timestamp::TIME)
        ) AS full_timestamp
    FROM dim_date_times
),
time_differences AS (
    SELECT 
        year,
        full_timestamp,
        LEAD(full_timestamp) OVER (PARTITION BY year ORDER BY full_timestamp) AS next_timestamp
    FROM ordered_timestamps
)
SELECT 
    year,
    TO_CHAR(
        INTERVAL '1 second' * AVG(EXTRACT(EPOCH FROM (next_timestamp - full_timestamp))),
        'HH24:MI:SS'
    ) AS actual_time_taken
FROM time_differences
WHERE next_timestamp IS NOT NULL
GROUP BY year
ORDER BY actual_time_taken DESC
LIMIT 5;













































































