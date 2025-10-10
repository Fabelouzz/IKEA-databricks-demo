-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 08_silver_api_transform
-- MAGIC Transform bronze API data to clean silver dimensions and facts with:
-- MAGIC - CTE-based logic for readability
-- MAGIC - Explicit primary/composite keys
-- MAGIC - Referential integrity examples
-- MAGIC - Deduplication logic

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Silver FX Daily (CTE pattern with dedup)

-- COMMAND ----------

-- PURPOSE: Transform raw FX rates into clean daily rates with deduplication
-- PATTERN: Uses a 3-step CTE chain for readability (clean → dedup → final)
-- OUTPUT: One row per (currency_pair, date) with the latest rate

CREATE OR REPLACE TABLE silver.fx_rates_daily
COMMENT 'Daily FX rates with explicit composite PK: (pair, as_of_date)'
AS

-- CTE 1: Clean and standardize
-- Combines base and quote currencies into a single "pair" field (e.g., "EUR/SEK")
-- Filters out invalid data (nulls, non-positive rates)
WITH clean_rates AS (
  SELECT 
    as_of_date,
    -- CONCAT: Combines strings with a separator (creates "EUR/SEK" format)
    CONCAT(base_currency, '/', quote_currency) AS pair,
    rate,
    ingested_at,
    source_url
  FROM bronze.fx_rates_raw
  WHERE rate > 0  -- Data quality: exclude invalid rates (must be positive)
    AND as_of_date IS NOT NULL      -- Exclude rows missing dates
    AND base_currency IS NOT NULL   -- Exclude rows missing base currency
    AND quote_currency IS NOT NULL  -- Exclude rows missing quote currency
),

-- CTE 2: Deduplicate using window function
-- If multiple ingestions exist for same (pair, date), rank them by ingestion time
-- ROW_NUMBER assigns a unique rank within each partition
deduped AS (
  SELECT 
    as_of_date,
    pair,
    rate,
    -- ROW_NUMBER() creates a sequential number for each row within partitions
    -- PARTITION BY: Groups rows by (pair, as_of_date) - like GROUP BY but keeps all rows
    -- ORDER BY: Within each partition, rank by newest ingestion first
    -- Result: rn=1 is the latest ingestion for each (pair, date)
    ROW_NUMBER() OVER (
      PARTITION BY pair, as_of_date 
      ORDER BY ingested_at DESC  -- DESC = descending (newest first)
    ) AS rn
  FROM clean_rates
),

-- CTE 3: Keep only the latest record for each (pair, date)
-- Filters to rn=1, which is the most recent ingestion
latest_by_day AS (
  SELECT 
    as_of_date,
    pair,
    rate
  FROM deduped
  WHERE rn = 1  -- Keep only the first-ranked (latest) row per partition
)

-- Final SELECT: Creates the silver table
-- CURRENT_TIMESTAMP() adds an audit column showing when the transform ran
SELECT 
  pair,              -- Composite key part 1: e.g., "EUR/SEK"
  as_of_date,        -- Composite key part 2: e.g., "2025-07-18"
  rate,              -- The exchange rate value
  CURRENT_TIMESTAMP() AS transformed_at  -- Audit: when this row was created
FROM latest_by_day
-- PRIMARY KEY (pair, as_of_date)  
-- Note: This is conceptual documentation. Delta Lake doesn't enforce PKs like traditional DBs,
-- but we document the intended uniqueness constraint for clarity
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Test: Verify no duplicates on composite key

-- COMMAND ----------

-- TEST 1: Check for duplicate composite keys
-- PURPOSE: Verify that (pair, as_of_date) is unique - no duplicates allowed
-- EXPECTED: 0 rows returned (empty result = PASS)

SELECT 
  pair, 
  as_of_date, 
  COUNT(*) AS cnt 
FROM silver.fx_rates_daily
GROUP BY pair, as_of_date           -- Group rows by composite key
HAVING COUNT(*) > 1;                -- HAVING filters groups (like WHERE but for aggregates)
                                     -- Only show groups with more than 1 row (duplicates)

-- COMMAND ----------

-- TEST 2: Count total duplicates found
-- PURPOSE: Numeric check - should return 0
-- PATTERN: Wraps previous query in a subquery and counts results

SELECT COUNT(*) AS duplicate_count
FROM (
  -- Subquery: Find all duplicate (pair, date) combinations
  SELECT pair, as_of_date, COUNT(*) AS cnt 
  FROM silver.fx_rates_daily
  GROUP BY pair, as_of_date
  HAVING COUNT(*) > 1  -- Only duplicates
);
-- EXPECTED OUTPUT: duplicate_count = 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Silver Dim Currency (referential integrity example)

-- COMMAND ----------

-- PURPOSE: Create a currency dimension table (lookup table)
-- PATTERN: Small reference table that other tables can join to
-- USE CASE: Provides human-readable currency names for reporting

CREATE OR REPLACE TABLE silver.dim_currency
COMMENT 'Currency dimension with PK: currency_code'
AS
SELECT DISTINCT  -- DISTINCT removes duplicate currency codes
  quote_currency AS currency_code,
  -- CASE statement: SQL's if-then-else logic
  -- Converts currency codes (SEK) to readable names (Swedish Krona)
  CASE 
    WHEN quote_currency = 'SEK' THEN 'Swedish Krona'
    WHEN quote_currency = 'USD' THEN 'US Dollar'
    WHEN quote_currency = 'EUR' THEN 'Euro'
    WHEN quote_currency = 'GBP' THEN 'British Pound'
    WHEN quote_currency = 'JPY' THEN 'Japanese Yen'
    ELSE quote_currency  -- Fallback: use code if not in list
  END AS currency_name,
  CURRENT_TIMESTAMP() AS created_at  -- Audit column
FROM bronze.fx_rates_raw
WHERE quote_currency IS NOT NULL  -- Exclude null currencies
-- PRIMARY KEY (currency_code)
-- Expected rows: 2-3 (SEK, USD, maybe EUR depending on API data)
;

-- COMMAND ----------

-- DISPLAY the created currency dimension
-- PURPOSE: Show what was created in the previous cell
-- EXPECTED: 2-5 rows (SEK, USD, EUR, etc.)

SELECT * FROM silver.dim_currency
ORDER BY currency_code;

-- Result explanation: This should show all currencies found in the FX data
-- with their human-readable names

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Test: Verify FK relationship (all FX pairs reference dim_currency)

-- COMMAND ----------

-- TEST 3: Referential Integrity Check (Foreign Key validation)
-- PURPOSE: Verify all currencies in fx_rates_daily exist in dim_currency
-- CONCEPT: This simulates a FOREIGN KEY constraint (not enforced in Delta Lake)
-- EXPECTED: 0 rows (all FX currencies have a matching dim record)

SELECT 
  SPLIT(pair, '/')[1] AS quote_currency,  -- SPLIT: Breaks "EUR/SEK" into ["EUR", "SEK"]
                                           -- [1] gets second element (SEK)
  COUNT(*) AS rate_count
FROM silver.fx_rates_daily
-- NOT IN: Finds rows where quote_currency doesn't exist in dim_currency
-- Subquery returns all valid currency_codes
WHERE SPLIT(pair, '/')[1] NOT IN (SELECT currency_code FROM silver.dim_currency)
GROUP BY SPLIT(pair, '/')[1];
-- If this returns rows, it means "orphan" currencies exist (referential integrity violation)

-- COMMAND ----------

-- Comment explaining expected result
-- EXPECTED: 0 rows returned = all FX rates reference valid currencies
-- This proves referential integrity is maintained (like a foreign key constraint)

-- COMMAND ----------

-- DISPLAY a sample of the fx_rates_daily table
-- PURPOSE: Show what the cleaned FX data looks like
-- EXPECTED: Multiple rows with pair, as_of_date, rate

SELECT * FROM silver.fx_rates_daily
ORDER BY pair, as_of_date DESC
LIMIT 20;

-- Result explanation: Shows deduplicated FX rates with composite key (pair, as_of_date)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Silver Dim Products (from DummyJSON)

-- COMMAND ----------

-- PURPOSE: Transform DummyJSON products into a clean product dimension
-- BUSINESS LOGIC: Adds "size_class" field to categorize products (LARGE vs SMALL)
-- PATTERN: CTE for cleaning + deduplication, similar to FX rates above

CREATE OR REPLACE TABLE silver.dim_products_api
COMMENT 'Product dimension from DummyJSON with PK: product_id'
AS

-- CTE 1: Clean and add business logic
WITH clean_products AS (
  SELECT 
    id AS product_id,  -- Rename for clarity
    title,
    description,
    category,
    brand,
    price,
    stock,
    rating,
    -- Business logic: Classify products by price point
    -- Maps to IKEA concept: large furniture (>1000) vs small items (≤1000)
    CASE 
      WHEN price > 1000 THEN 'LARGE'  -- High-value items (sofas, beds, etc.)
      ELSE 'SMALL'                     -- Lower-value items (lamps, decor, etc.)
    END AS size_class,
    ingested_at
  FROM bronze.products_raw
  WHERE id IS NOT NULL  -- Data quality: exclude products without IDs
),

-- CTE 2: Deduplicate products (in case of multiple ingestions)
-- Same pattern as FX rates: keep latest ingestion per product_id
deduped_products AS (
  SELECT 
    *,  -- Select all columns from clean_products
    -- Window function: Rank products by ingestion time within each product_id
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY ingested_at DESC) AS rn
  FROM clean_products
)

-- Final SELECT: Keep only the latest version of each product
SELECT 
  product_id,        -- PK
  title,
  description,
  category,
  brand,
  price,
  stock,
  rating,
  size_class,        -- Derived field from business logic
  CURRENT_TIMESTAMP() AS transformed_at
FROM deduped_products
WHERE rn = 1  -- Latest ingestion only
-- PRIMARY KEY (product_id)
-- Expected rows: 194 (all DummyJSON products)
;

-- COMMAND ----------

-- DISPLAY the created product dimension
-- PURPOSE: Show sample products with size_class classification
-- EXPECTED: ~194 rows (all DummyJSON products)

SELECT 
  product_id,
  title,
  category,
  brand,
  price,
  size_class,
  rating,
  stock
FROM silver.dim_products_api
ORDER BY price DESC
LIMIT 20;

-- Result explanation: Shows products classified as LARGE (price > 1000) or SMALL (≤1000)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Test: Verify PK uniqueness and size_class logic

-- COMMAND ----------

-- TEST 4: Check for duplicate product_ids
-- PURPOSE: Verify product_id is unique (primary key constraint)
-- EXPECTED: duplicate_product_count = 0

SELECT COUNT(*) AS duplicate_product_count
FROM (
  -- Subquery: Find product_ids that appear more than once
  SELECT product_id, COUNT(*) 
  FROM silver.dim_products_api
  GROUP BY product_id
  HAVING COUNT(*) > 1  -- Only show duplicates
);
-- EXPECTED OUTPUT: 0 duplicates

-- COMMAND ----------

-- TEST 5: Verify size_class business logic
-- PURPOSE: Validate the CASE statement worked correctly
-- EXPECTED: LARGE products have min_price > 1000, SMALL products have max_price ≤ 1000
-- This is the FIRST query that shows actual data (not just validation)

SELECT 
  size_class,
  COUNT(*) AS product_count,      -- How many products in each category
  MIN(price) AS min_price,         -- Cheapest product in category
  MAX(price) AS max_price,         -- Most expensive product in category
  AVG(price) AS avg_price          -- Average price (mean)
FROM silver.dim_products_api
GROUP BY size_class               -- Separate statistics for LARGE vs SMALL
ORDER BY size_class;              -- Alphabetical order (LARGE, then SMALL)
-- EXPECTED: 
-- LARGE | ~20-30 products | min > 1000 | max ~1500 | avg ~1100-1200
-- SMALL | ~170 products   | min < 1000 | max ≤ 1000 | avg ~100-500

-- COMMAND ----------

-- Expected: LARGE has min_price > 1000, SMALL has max_price <= 1000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Silver Dim Customers (from DummyJSON users)

-- COMMAND ----------

-- PURPOSE: Create customer dimension from DummyJSON users
-- SIMPLER PATTERN: No CTEs needed since data is already clean
-- TRANSFORMATION: Combines first_name + last_name into single field

CREATE OR REPLACE TABLE silver.dim_customers_api
COMMENT 'Customer dimension from DummyJSON with PK: customer_id'
AS
SELECT 
  id AS customer_id,  -- Rename for consistency
  -- CONCAT: Combines first and last name with a space
  -- Example: "John" + " " + "Doe" = "John Doe"
  CONCAT(first_name, ' ', last_name) AS customer_name,
  email,
  age,
  gender,
  CURRENT_TIMESTAMP() AS transformed_at  -- Audit column
FROM bronze.users_raw
WHERE id IS NOT NULL  -- Data quality: exclude users without IDs
-- PRIMARY KEY (customer_id)
-- Expected rows: 208 (all DummyJSON users)
;

-- COMMAND ----------

-- DISPLAY the created customer dimension
-- PURPOSE: Show sample customers with concatenated names
-- EXPECTED: ~208 rows (all DummyJSON users)

SELECT 
  customer_id,
  customer_name,
  email,
  age,
  gender
FROM silver.dim_customers_api
ORDER BY customer_id
LIMIT 20;

-- Result explanation: Shows users with full names (first_name + " " + last_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Test: Verify PK uniqueness and data quality

-- COMMAND ----------

-- TEST 6: Check for duplicate customer_ids
-- PURPOSE: Verify customer_id uniqueness (primary key)
-- EXPECTED: 0 duplicates

SELECT COUNT(*) AS duplicate_customer_count
FROM (
  -- Find customer_ids that appear more than once
  SELECT customer_id, COUNT(*) 
  FROM silver.dim_customers_api
  GROUP BY customer_id
  HAVING COUNT(*) > 1
);
-- EXPECTED OUTPUT: duplicate_customer_count = 0

-- COMMAND ----------

-- TEST 7: Check for nulls in critical fields
-- PURPOSE: Verify data completeness (no missing values in key columns)
-- PATTERN: COUNT(*) counts all rows, COUNT(column) counts non-null values
-- If they're equal, no nulls exist

SELECT 
  COUNT(*) AS total_customers,           -- Total rows in table
  COUNT(customer_id) AS non_null_ids,    -- Rows with non-null customer_id
  COUNT(customer_name) AS non_null_names,-- Rows with non-null customer_name
  COUNT(email) AS non_null_emails        -- Rows with non-null email
FROM silver.dim_customers_api;
-- EXPECTED: All counts should be equal (e.g., all = 208)
-- If non_null_ids < total_customers, some IDs are null (data quality issue)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Summary Statistics & Validation

-- COMMAND ----------

-- TEST 8: Summary of all silver tables
-- PURPOSE: Quick overview showing all 4 tables were created successfully
-- PATTERN: UNION ALL combines multiple SELECT statements into one result set
-- Useful for: Verifying pipeline completion, monitoring table sizes

SELECT 'fx_rates_daily' AS table_name, COUNT(*) AS row_count FROM silver.fx_rates_daily
-- UNION ALL: Combines results from multiple queries (keeps duplicates)
-- vs UNION (removes duplicates, slower)
UNION ALL SELECT 'dim_currency', COUNT(*) FROM silver.dim_currency
UNION ALL SELECT 'dim_products_api', COUNT(*) FROM silver.dim_products_api
UNION ALL SELECT 'dim_customers_api', COUNT(*) FROM silver.dim_customers_api
ORDER BY table_name;  -- Sort alphabetically by table name
-- EXPECTED OUTPUT:
-- dim_currency       | 2-3
-- dim_customers_api  | 208
-- dim_products_api   | 194
-- fx_rates_daily     | ~128

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Expected Counts:
-- MAGIC - `fx_rates_daily`: ~128 rows (depends on date range, 90 days × ~2 currencies minus weekends)
-- MAGIC - `dim_currency`: 2-3 rows (SEK, USD, possibly EUR)
-- MAGIC - `dim_products_api`: 194 rows (all DummyJSON products)
-- MAGIC - `dim_customers_api`: 208 rows (all DummyJSON users)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6. Final Quality Checks

-- COMMAND ----------

-- FINAL CHECK 1: Verify all silver tables exist
-- PURPOSE: Catalog-level validation - shows all tables in the silver schema
-- USEFUL FOR: Confirming pipeline created all expected tables

SHOW TABLES IN silver;
-- EXPECTED: 4 tables listed (dim_currency, dim_customers_api, dim_products_api, fx_rates_daily)

-- COMMAND ----------

-- FINAL CHECK 2: Sample FX rates data
-- PURPOSE: Visual inspection of data quality
-- LIMIT 3: Shows first 3 rows (small sample for quick review)

SELECT 'fx_rates_daily' AS source, pair, as_of_date, rate 
FROM silver.fx_rates_daily 
LIMIT 3;
-- EXPECTED: Rows like EUR/SEK | 2025-07-18 | 11.48

-- COMMAND ----------

-- FINAL CHECK 3: Sample currency dimension
-- PURPOSE: Show all currencies (small table, no LIMIT needed)

SELECT 'dim_currency' AS source, currency_code, currency_name 
FROM silver.dim_currency;
-- EXPECTED: 2-3 rows (SEK | Swedish Krona, USD | US Dollar, etc.)

-- COMMAND ----------

-- FINAL CHECK 4: Sample products
-- PURPOSE: Verify product data and size_class mapping

SELECT 'dim_products_api' AS source, product_id, title, category, size_class, price 
FROM silver.dim_products_api 
LIMIT 5;
-- EXPECTED: Mix of LARGE and SMALL products with appropriate prices

-- COMMAND ----------

-- FINAL CHECK 5: Sample customers
-- PURPOSE: Verify customer_name concatenation and data completeness

SELECT 'dim_customers_api' AS source, customer_id, customer_name, email, gender 
FROM silver.dim_customers_api 
LIMIT 5;
-- EXPECTED: Full names (first + last), valid emails, gender values

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Summary
-- MAGIC 
-- MAGIC ✓ **CTEs used for readability**: clean → dedup → latest pattern  
-- MAGIC ✓ **Composite keys documented**: (pair, as_of_date) for fx_rates_daily  
-- MAGIC ✓ **Primary keys documented**: product_id, customer_id, currency_code  
-- MAGIC ✓ **Referential integrity**: fx rates reference dim_currency  
-- MAGIC ✓ **Deduplication logic**: ROW_NUMBER() with PARTITION BY  
-- MAGIC ✓ **Data quality checks**: null filters, positive rates, unique keys  
-- MAGIC ✓ **Business logic**: size_class mapping based on price threshold

