-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 12_bi_concepts_demo - Engineering Concepts Dashboard
-- MAGIC 
-- MAGIC This notebook contains queries to **visualize data engineering concepts**, not just business KPIs.
-- MAGIC 
-- MAGIC **Use this in Databricks Lakeview** to build a multi-tab dashboard that proves:
-- MAGIC - Delta governance works (time travel, history)
-- MAGIC - Performance tuning delivers results (measurable speedup)
-- MAGIC - Metadata-driven architecture is scalable
-- MAGIC - Data quality gates catch issues
-- MAGIC - API ingestion is reliable
-- MAGIC 
-- MAGIC **For interviews**: This shows you understand systems, not just SQL.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SECTION 1: Delta Governance & Time Travel

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 1.1: Full Delta History (Audit Trail)
-- MAGIC 
-- MAGIC **Purpose**: Show all operations on gold.baskets_enriched  
-- MAGIC **For Lakeview**: Table visualization, sortable by timestamp

-- COMMAND ----------

DESCRIBE HISTORY gold.baskets_enriched;

-- Expected columns:
-- - version: Integer version number
-- - timestamp: When operation occurred
-- - operation: CREATE TABLE, WRITE, DELETE, MERGE, etc.
-- - operationParameters: JSON with WHERE clause, mode, etc.
-- - operationMetrics: Rows added/deleted, files written, etc.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 1.2: Operation Summary (Grouped)
-- MAGIC 
-- MAGIC **Purpose**: Count operations by type  
-- MAGIC **For Lakeview**: Bar chart (operation type vs count)

-- COMMAND ----------

SELECT 
  operation,
  COUNT(*) AS operation_count,
  MIN(timestamp) AS first_occurrence,
  MAX(timestamp) AS last_occurrence
FROM (
  DESCRIBE HISTORY gold.baskets_enriched
)
GROUP BY operation
ORDER BY operation_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 1.3: Table Size Over Time
-- MAGIC 
-- MAGIC **Purpose**: Track table growth/shrinkage across versions  
-- MAGIC **For Lakeview**: Line chart (version vs rows)

-- COMMAND ----------

SELECT 
  version,
  timestamp,
  operation,
  CAST(operationMetrics.numOutputRows AS BIGINT) AS output_rows,
  CAST(operationMetrics.numRemovedFiles AS INT) AS removed_files,
  CAST(operationMetrics.numAddedFiles AS INT) AS added_files
FROM (
  DESCRIBE HISTORY gold.baskets_enriched
)
WHERE operationMetrics.numOutputRows IS NOT NULL
ORDER BY version;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 1.4: Data Quality Violations Catalog
-- MAGIC 
-- MAGIC **Purpose**: Show corrupted rows detected and deleted  
-- MAGIC **For Lakeview**: Table with conditional formatting (severity colors)

-- COMMAND ----------

SELECT 
  violation_type,
  severity,
  COUNT(*) AS violation_count,
  COLLECT_LIST(description) AS examples,
  MAX(detected_at) AS last_detected
FROM ops.data_quality_violations
GROUP BY violation_type, severity
ORDER BY 
  CASE severity 
    WHEN 'CRITICAL' THEN 1 
    WHEN 'HIGH' THEN 2 
    WHEN 'MEDIUM' THEN 3 
    ELSE 4 
  END,
  violation_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 1.5: Violations by Receipt ID (Detail View)
-- MAGIC 
-- MAGIC **Purpose**: Show individual corrupted rows for audit  
-- MAGIC **For Lakeview**: Detail table (drilldown from Query 1.4)

-- COMMAND ----------

SELECT 
  receipt_id,
  violation_type,
  severity,
  description,
  detected_at
FROM ops.data_quality_violations
ORDER BY severity, detected_at DESC
LIMIT 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SECTION 2: Performance Tuning Evidence

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 2.1: Latest Performance Run
-- MAGIC 
-- MAGIC **Purpose**: Show most recent speedup result  
-- MAGIC **For Lakeview**: KPI tile (speedup_factor as big number)

-- COMMAND ----------

SELECT 
  test_name,
  naive_time_s,
  optimized_time_s,
  ROUND(speedup_factor, 1) AS speedup_x,
  fact_rows,
  dim_rows,
  skew_description,
  run_timestamp
FROM ops.perf_runs
ORDER BY run_timestamp DESC
LIMIT 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 2.2: Performance Comparison (Bar Chart)
-- MAGIC 
-- MAGIC **Purpose**: Side-by-side comparison of naive vs optimized  
-- MAGIC **For Lakeview**: Grouped bar chart (test_name, naive vs optimized time)

-- COMMAND ----------

SELECT 
  DATE(run_timestamp) AS run_date,
  test_name,
  naive_time_s,
  optimized_time_s,
  naive_time_s - optimized_time_s AS time_saved_s
FROM ops.perf_runs
ORDER BY run_timestamp DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 2.3: Speedup Trend Over Time
-- MAGIC 
-- MAGIC **Purpose**: Track performance improvements across test runs  
-- MAGIC **For Lakeview**: Line chart (run_timestamp vs speedup_factor)

-- COMMAND ----------

SELECT 
  run_timestamp,
  test_name,
  speedup_factor,
  naive_plan_type,
  optimized_plan_type
FROM ops.perf_runs
ORDER BY run_timestamp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 2.4: Spark Config Comparison
-- MAGIC 
-- MAGIC **Purpose**: Show what configs changed between naive and optimized  
-- MAGIC **For Lakeview**: Table (2 columns: naive_config, optimized_config)

-- COMMAND ----------

SELECT 
  'Naive Config' AS scenario,
  naive_config AS configuration,
  naive_plan_type AS join_type,
  naive_time_s AS execution_time_s
FROM (
  SELECT naive_config, naive_plan_type, naive_time_s
  FROM ops.perf_runs
  WHERE test_name = 'extreme_skew_join_demo'
  ORDER BY run_timestamp DESC
  LIMIT 1
)

UNION ALL

SELECT 
  'Optimized Config' AS scenario,
  optimized_config AS configuration,
  optimized_plan_type AS join_type,
  optimized_time_s AS execution_time_s
FROM (
  SELECT optimized_config, optimized_plan_type, optimized_time_s
  FROM ops.perf_runs
  WHERE test_name = 'extreme_skew_join_demo'
  ORDER BY run_timestamp DESC
  LIMIT 1
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SECTION 3: Metadata-Driven Joins Proof

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 3.1: Join Output Stats
-- MAGIC 
-- MAGIC **Purpose**: Show the result of metadata-driven joins  
-- MAGIC **For Lakeview**: KPI tiles (row count, column count)

-- COMMAND ----------

SELECT 
  COUNT(*) AS total_rows,
  COUNT(DISTINCT receipt_id) AS unique_receipts,
  COUNT(DISTINCT customer_id) AS unique_customers,
  SUM(CASE WHEN pair IS NOT NULL THEN 1 ELSE 0 END) AS rows_with_fx,
  COUNT(*) - COUNT(customer_name) AS rows_missing_customer_name
FROM gold.baskets_enriched;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 3.2: Column Provenance (Which Source?)
-- MAGIC 
-- MAGIC **Purpose**: Show which columns came from which table  
-- MAGIC **For Lakeview**: Table (column_name, source_table)

-- COMMAND ----------

-- Manual mapping (in production, could parse from YAML or information_schema)
SELECT 'receipt_id' AS column_name, 'silver.baskets (base)' AS source
UNION ALL SELECT 'loyalty_id', 'silver.baskets (base)'
UNION ALL SELECT 'date', 'silver.baskets (base)'
UNION ALL SELECT 'store_id', 'silver.baskets (base)'
UNION ALL SELECT 'attached', 'silver.baskets (base)'
UNION ALL SELECT 'customer_id', 'silver.dim_customers_api (join 1)'
UNION ALL SELECT 'customer_name', 'silver.dim_customers_api (join 1)'
UNION ALL SELECT 'age', 'silver.dim_customers_api (join 1)'
UNION ALL SELECT 'gender', 'silver.dim_customers_api (join 1)'
UNION ALL SELECT 'email', 'silver.dim_customers_api (join 1)'
UNION ALL SELECT 'pair', 'silver.fx_rates_daily (join 2)'
UNION ALL SELECT 'rate', 'silver.fx_rates_daily (join 2)'
ORDER BY source, column_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 3.3: Broadcast Join Effectiveness
-- MAGIC 
-- MAGIC **Purpose**: Show join coverage (how many rows matched)  
-- MAGIC **For Lakeview**: Stacked bar chart (matched vs unmatched)

-- COMMAND ----------

SELECT 
  CASE 
    WHEN customer_name IS NOT NULL THEN 'Customer Matched'
    ELSE 'Customer Not Matched'
  END AS customer_join_status,
  CASE 
    WHEN pair IS NOT NULL THEN 'FX Matched'
    ELSE 'FX Not Matched'
  END AS fx_join_status,
  COUNT(*) AS row_count
FROM gold.baskets_enriched
GROUP BY customer_join_status, fx_join_status
ORDER BY row_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SECTION 4: Data Quality Monitor

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 4.1: Composite Key Uniqueness Check
-- MAGIC 
-- MAGIC **Purpose**: Ensure (pair, as_of_date) is unique in silver.fx_rates_daily  
-- MAGIC **For Lakeview**: KPI tile (should be 0, red if > 0)

-- COMMAND ----------

SELECT COUNT(*) AS duplicate_composite_keys
FROM (
  SELECT pair, as_of_date, COUNT(*) AS cnt
  FROM silver.fx_rates_daily
  GROUP BY pair, as_of_date
  HAVING COUNT(*) > 1
);

-- Expected: 0 (no duplicates)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 4.2: Rate Bounds Validation
-- MAGIC 
-- MAGIC **Purpose**: Ensure all FX rates are positive  
-- MAGIC **For Lakeview**: KPI tile (should be 0, red if > 0)

-- COMMAND ----------

SELECT COUNT(*) AS invalid_rates
FROM silver.fx_rates_daily
WHERE rate <= 0 OR rate IS NULL;

-- Expected: 0 (all rates valid)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 4.3: Null Primary Key Check
-- MAGIC 
-- MAGIC **Purpose**: Ensure product_id is never null  
-- MAGIC **For Lakeview**: KPI tile (should be 0, red if > 0)

-- COMMAND ----------

SELECT COUNT(*) AS null_product_ids
FROM silver.dim_products_api
WHERE product_id IS NULL;

-- Expected: 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 4.4: Orphan Foreign Key Detection
-- MAGIC 
-- MAGIC **Purpose**: Find baskets with customer_id not in dimension  
-- MAGIC **For Lakeview**: KPI tile (should be 0 after cleanup)

-- COMMAND ----------

SELECT COUNT(*) AS orphan_customer_ids
FROM gold.baskets_enriched base
WHERE NOT EXISTS (
  SELECT 1 FROM silver.dim_customers_api cust
  WHERE base.customer_id = cust.customer_id
);

-- Expected: 0 (no orphans after DELETE operations)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 4.5: Date Range Validation
-- MAGIC 
-- MAGIC **Purpose**: Ensure all dates are within expected range (2020-2026)  
-- MAGIC **For Lakeview**: KPI tile (should be 0, red if > 0)

-- COMMAND ----------

SELECT COUNT(*) AS out_of_range_dates
FROM gold.baskets_enriched
WHERE date < '2020-01-01' OR date > '2026-12-31';

-- Expected: 0 (after DELETE operation 3)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 4.6: Quality Gate Summary Dashboard
-- MAGIC 
-- MAGIC **Purpose**: All-in-one quality check dashboard  
-- MAGIC **For Lakeview**: Table with conditional formatting (all green if 0)

-- COMMAND ----------

SELECT 'FX Composite Key Duplicates' AS quality_check, 
       (SELECT COUNT(*) FROM (SELECT pair, as_of_date FROM silver.fx_rates_daily GROUP BY pair, as_of_date HAVING COUNT(*) > 1)) AS violation_count,
       'Should be 0' AS expected
UNION ALL
SELECT 'FX Invalid Rates', 
       (SELECT COUNT(*) FROM silver.fx_rates_daily WHERE rate <= 0 OR rate IS NULL),
       'Should be 0'
UNION ALL
SELECT 'Product Null PKs',
       (SELECT COUNT(*) FROM silver.dim_products_api WHERE product_id IS NULL),
       'Should be 0'
UNION ALL
SELECT 'Orphan Customer FKs',
       (SELECT COUNT(*) FROM gold.baskets_enriched WHERE customer_id NOT IN (SELECT customer_id FROM silver.dim_customers_api)),
       'Should be 0'
UNION ALL
SELECT 'Out-of-Range Dates',
       (SELECT COUNT(*) FROM gold.baskets_enriched WHERE date < '2020-01-01' OR date > '2026-12-31'),
       'Should be 0';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SECTION 5: API Ingestion Operations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 5.1: Bronze Landed Counts
-- MAGIC 
-- MAGIC **Purpose**: Show rows landed from each API source  
-- MAGIC **For Lakeview**: Bar chart (source vs count)

-- COMMAND ----------

SELECT 'DummyJSON Products' AS source, COUNT(*) AS row_count FROM bronze.products_raw
UNION ALL
SELECT 'DummyJSON Users', COUNT(*) FROM bronze.users_raw
UNION ALL
SELECT 'Frankfurter FX Rates', COUNT(*) FROM bronze.fx_rates_raw
ORDER BY row_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 5.2: FX Coverage Calendar
-- MAGIC 
-- MAGIC **Purpose**: Show which dates have FX data (heatmap-ready)  
-- MAGIC **For Lakeview**: Calendar heatmap or line chart

-- COMMAND ----------

SELECT 
  as_of_date,
  COUNT(DISTINCT pair) AS pair_count,
  COLLECT_SET(pair) AS pairs_available
FROM bronze.fx_rates_raw
GROUP BY as_of_date
ORDER BY as_of_date DESC
LIMIT 90;  -- Last 90 days

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 5.3: Ingestion Freshness
-- MAGIC 
-- MAGIC **Purpose**: When was data last ingested?  
-- MAGIC **For Lakeview**: KPI tile (latest ingested_at timestamp)

-- COMMAND ----------

SELECT 
  'Products' AS source,
  MAX(ingested_at) AS latest_ingestion,
  DATEDIFF(NOW(), MAX(ingested_at)) AS days_since_ingest
FROM bronze.products_raw

UNION ALL

SELECT 
  'Users',
  MAX(ingested_at),
  DATEDIFF(NOW(), MAX(ingested_at))
FROM bronze.users_raw

UNION ALL

SELECT 
  'FX Rates',
  MAX(ingested_at),
  DATEDIFF(NOW(), MAX(ingested_at))
FROM bronze.fx_rates_raw

ORDER BY days_since_ingest;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 5.4: Pagination Completeness Check
-- MAGIC 
-- MAGIC **Purpose**: Compare bronze counts to API totals (manual entry for demo)  
-- MAGIC **For Lakeview**: Table with % complete

-- COMMAND ----------

-- Manual comparison (API total would be fetched in production)
SELECT 
  'products' AS entity,
  COUNT(*) AS bronze_count,
  194 AS api_total,  -- From DummyJSON API (as of demo date)
  ROUND((COUNT(*) * 100.0) / 194, 1) AS completion_pct
FROM bronze.products_raw

UNION ALL

SELECT 
  'users',
  COUNT(*),
  200,  -- Estimated API total
  ROUND((COUNT(*) * 100.0) / 200, 1)
FROM bronze.users_raw

UNION ALL

SELECT 
  'fx_rates',
  COUNT(*),
  128,  -- ~90 days of weekdays
  ROUND((COUNT(*) * 100.0) / 128, 1)
FROM bronze.fx_rates_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SECTION 6: Transform Validation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 6.1: Deduplication Effectiveness
-- MAGIC 
-- MAGIC **Purpose**: Show products were deduplicated correctly  
-- MAGIC **For Lakeview**: KPI tile (should be 0 duplicates)

-- COMMAND ----------

SELECT COUNT(*) AS duplicate_product_ids
FROM (
  SELECT product_id, COUNT(*) AS cnt
  FROM silver.dim_products_api
  GROUP BY product_id
  HAVING COUNT(*) > 1
);

-- Expected: 0

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 6.2: Size Class Distribution
-- MAGIC 
-- MAGIC **Purpose**: Show LARGE vs SMALL product classification  
-- MAGIC **For Lakeview**: Pie chart

-- COMMAND ----------

SELECT 
  size_class,
  COUNT(*) AS product_count,
  ROUND(AVG(price), 2) AS avg_price,
  MIN(price) AS min_price,
  MAX(price) AS max_price
FROM silver.dim_products_api
GROUP BY size_class
ORDER BY size_class;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 6.3: Transformation Pipeline Funnel
-- MAGIC 
-- MAGIC **Purpose**: Show row counts at each layer (bronze → silver → gold)  
-- MAGIC **For Lakeview**: Funnel chart

-- COMMAND ----------

SELECT 'Bronze: products_raw' AS stage, 1 AS stage_order, COUNT(*) AS row_count FROM bronze.products_raw
UNION ALL
SELECT 'Silver: dim_products_api', 2, COUNT(*) FROM silver.dim_products_api
UNION ALL
SELECT 'Bronze: baskets', 3, COUNT(*) FROM silver.baskets
UNION ALL
SELECT 'Gold: baskets_enriched', 4, COUNT(*) FROM gold.baskets_enriched
ORDER BY stage_order;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query 6.4: Sample Transformed Records
-- MAGIC 
-- MAGIC **Purpose**: Show examples of enriched data  
-- MAGIC **For Lakeview**: Sample table

-- COMMAND ----------

SELECT 
  receipt_id,
  date,
  customer_name,
  age,
  gender,
  pair AS fx_pair,
  ROUND(rate, 4) AS fx_rate,
  attached
FROM gold.baskets_enriched
WHERE customer_name IS NOT NULL
  AND pair IS NOT NULL
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## END OF DASHBOARD QUERIES
-- MAGIC 
-- MAGIC **Next steps**:
-- MAGIC 1. Create Databricks Lakeview dashboard
-- MAGIC 2. Add these queries as tiles organized by section
-- MAGIC 3. Apply visualizations (bar charts, line charts, KPIs, tables)
-- MAGIC 4. Add conditional formatting (red if violations > 0, green if = 0)
-- MAGIC 5. Screenshot each tab for README
-- MAGIC 
-- MAGIC **See**: `docs/LAKEVIEW_BUILD_GUIDE.md` for step-by-step dashboard assembly

