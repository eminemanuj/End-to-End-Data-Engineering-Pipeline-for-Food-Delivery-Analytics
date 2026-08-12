-- =====================================================================
-- Snowflake setup for the Zomato food delivery data pipeline
-- Run top to bottom in a Snowsight worksheet as ACCOUNTADMIN.
--
-- Before running: update the <BUCKET>, <AWS_KEY_ID>, <AWS_SECRET_KEY>,
-- and RSA public key placeholders below with your own values. Never
-- commit real credentials - this file is a template.
-- =====================================================================

-- =====================================================================
-- Phase 1 - Warehouse, database, schemas, role
-- =====================================================================
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS ZOMATO_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS ZOMATO;
CREATE SCHEMA IF NOT EXISTS ZOMATO.BRONZE;
CREATE SCHEMA IF NOT EXISTS ZOMATO.RAW;
CREATE SCHEMA IF NOT EXISTS ZOMATO.STAGING;
CREATE SCHEMA IF NOT EXISTS ZOMATO.MARTS;
CREATE SCHEMA IF NOT EXISTS ZOMATO.SNAPSHOTS;
CREATE SCHEMA IF NOT EXISTS ZOMATO.AI;

CREATE ROLE IF NOT EXISTS DBT_ROLE;
GRANT USAGE   ON WAREHOUSE ZOMATO_WH TO ROLE DBT_ROLE;
GRANT OPERATE ON WAREHOUSE ZOMATO_WH TO ROLE DBT_ROLE;
GRANT ALL     ON DATABASE  ZOMATO    TO ROLE DBT_ROLE;
GRANT ALL     ON ALL SCHEMAS IN DATABASE ZOMATO TO ROLE DBT_ROLE;
GRANT ALL     ON FUTURE SCHEMAS IN DATABASE ZOMATO TO ROLE DBT_ROLE;
GRANT ALL     ON FUTURE TABLES IN DATABASE ZOMATO TO ROLE DBT_ROLE;
GRANT ALL     ON FUTURE VIEWS  IN DATABASE ZOMATO TO ROLE DBT_ROLE;

-- Grant DBT_ROLE to your own user so you can act as it in worksheets/dbt/Airflow
SET my_user = CURRENT_USER();
GRANT ROLE DBT_ROLE TO USER IDENTIFIER($my_user);

SELECT 'Phase 1 complete' AS status;

-- =====================================================================
-- Phase 2 - Key-pair authentication
-- Generate a key pair locally first:
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
-- Then paste the public key body (no header/footer/newlines) below.
-- =====================================================================
ALTER USER IDENTIFIER($my_user) SET RSA_PUBLIC_KEY='<PASTE_PUBLIC_KEY_BODY_HERE>';

-- Verify:
-- DESC USER IDENTIFIER($my_user);  -- check RSA_PUBLIC_KEY_FP is populated

SELECT 'Phase 2 complete' AS status;

-- =====================================================================
-- Phase 3 - External stage on S3 (direct credentials, no AssumeRole)
-- =====================================================================
USE DATABASE ZOMATO;
USE SCHEMA RAW;

CREATE OR REPLACE FILE FORMAT ZOMATO.RAW.CSV_FMT
  TYPE = 'CSV'
  COMPRESSION = 'AUTO'
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  EMPTY_FIELD_AS_NULL = TRUE
  NULL_IF = ('', '\\N', 'NULL')
  TRIM_SPACE = FALSE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- Uses a plain IAM user's access key/secret directly on the stage, rather
-- than a STORAGE INTEGRATION + AssumeRole role chain. Simpler to reason
-- about for a personal project; the IAM user should have read-only
-- access scoped to just this bucket.
CREATE OR REPLACE STAGE ZOMATO.RAW.ZOMATO_RAW_STAGE
  URL = 's3://<BUCKET>/raw/'
  CREDENTIALS = (AWS_KEY_ID = '<AWS_KEY_ID>' AWS_SECRET_KEY = '<AWS_SECRET_KEY>')
  FILE_FORMAT = ZOMATO.RAW.CSV_FMT;

-- Confirm Snowflake can see the uploaded files (expects seven folders:
-- restaurants/ users/ food/ menu/ orders/ order_items/ reviews/)
LIST @ZOMATO.RAW.ZOMATO_RAW_STAGE;

SELECT 'Phase 3 complete' AS status;

-- =====================================================================
-- Phase 4 - RAW tables
-- Dimension tables (restaurants/users/food/menu) carry a leading unnamed
-- index column from the source CSVs, hence the _idx placeholder column.
-- =====================================================================
CREATE OR REPLACE TABLE RAW.restaurants (
  _idx STRING, id STRING, name STRING, city STRING, rating STRING,
  rating_count STRING, cost STRING, cuisine STRING, lic_no STRING,
  link STRING, address STRING, menu STRING
);

CREATE OR REPLACE TABLE RAW.users (
  _idx STRING, user_id STRING, name STRING, email STRING, password STRING, age STRING,
  gender STRING, marital_status STRING, occupation STRING, monthly_income STRING,
  education STRING, family_size STRING
);

CREATE OR REPLACE TABLE RAW.food (
  _idx STRING, f_id STRING, item STRING, veg_or_non_veg STRING
);

CREATE OR REPLACE TABLE RAW.menu (
  _idx STRING, menu_id STRING, r_id STRING, f_id STRING, cuisine STRING, price STRING
);

CREATE OR REPLACE TABLE RAW.orders (
  order_id NUMBER, order_timestamp TIMESTAMP_NTZ, order_date DATE, user_id NUMBER,
  r_id NUMBER, restaurant_city STRING, cuisine STRING, items_count NUMBER,
  sales_qty NUMBER, subtotal NUMBER, discount NUMBER, delivery_fee NUMBER, gst NUMBER,
  sales_amount NUMBER, currency STRING, payment_method STRING, order_status STRING,
  customer_rating NUMBER, delivery_time_min NUMBER
);

CREATE OR REPLACE TABLE RAW.order_items (
  order_item_id NUMBER, order_id NUMBER, r_id NUMBER, f_id STRING,
  price NUMBER, quantity NUMBER, line_amount NUMBER
);

CREATE OR REPLACE TABLE RAW.reviews (
  review_id NUMBER, order_id NUMBER, user_id NUMBER, restaurant_id NUMBER,
  rating NUMBER, comment STRING, review_date DATE
);

SELECT 'Phase 4 complete' AS status;

-- =====================================================================
-- Phase 5 - Load RAW from S3
-- Dimensions come from messier real-world source data, so bad rows are
-- skipped (CONTINUE). Fact tables are generated/clean, so loads stay
-- strict (ABORT_STATEMENT) to catch any real data-quality problems.
-- =====================================================================
USE WAREHOUSE ZOMATO_WH;

COPY INTO RAW.restaurants FROM @ZOMATO_RAW_STAGE/restaurants/  ON_ERROR = 'CONTINUE';
COPY INTO RAW.users       FROM @ZOMATO_RAW_STAGE/users/        ON_ERROR = 'CONTINUE';
COPY INTO RAW.food        FROM @ZOMATO_RAW_STAGE/food/         ON_ERROR = 'CONTINUE';
COPY INTO RAW.menu        FROM @ZOMATO_RAW_STAGE/menu/         ON_ERROR = 'CONTINUE';
COPY INTO RAW.orders      FROM @ZOMATO_RAW_STAGE/orders/       ON_ERROR = 'ABORT_STATEMENT';
COPY INTO RAW.order_items FROM @ZOMATO_RAW_STAGE/order_items/  ON_ERROR = 'ABORT_STATEMENT';
COPY INTO RAW.reviews     FROM @ZOMATO_RAW_STAGE/reviews/      ON_ERROR = 'ABORT_STATEMENT';

-- Sanity check row counts
SELECT 'restaurants' t, COUNT(*) n FROM RAW.restaurants
UNION ALL SELECT 'users', COUNT(*) FROM RAW.users
UNION ALL SELECT 'food', COUNT(*) FROM RAW.food
UNION ALL SELECT 'menu', COUNT(*) FROM RAW.menu
UNION ALL SELECT 'orders', COUNT(*) FROM RAW.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM RAW.order_items
UNION ALL SELECT 'reviews', COUNT(*) FROM RAW.reviews
ORDER BY t;

SELECT 'Phase 5 complete' AS status;

-- =====================================================================
-- Phase 6 - Grants for DBT_ROLE
-- Covers stage/file-format access (needed for COPY INTO via Airflow)
-- and read access across the schemas dbt builds into.
-- =====================================================================
GRANT USAGE ON SCHEMA ZOMATO.RAW TO ROLE DBT_ROLE;
GRANT USAGE ON ALL FILE FORMATS IN SCHEMA ZOMATO.RAW TO ROLE DBT_ROLE;
GRANT READ  ON ALL STAGES IN SCHEMA ZOMATO.RAW TO ROLE DBT_ROLE;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA ZOMATO.RAW TO ROLE DBT_ROLE;

GRANT USAGE ON SCHEMA ZOMATO.STAGING TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ZOMATO.STAGING TO ROLE DBT_ROLE;

GRANT USAGE ON SCHEMA ZOMATO.MARTS TO ROLE DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ZOMATO.MARTS TO ROLE DBT_ROLE;
GRANT SELECT ON ALL VIEWS  IN SCHEMA ZOMATO.MARTS TO ROLE DBT_ROLE;

SELECT 'Phase 6 complete - setup finished' AS status;