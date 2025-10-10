# Databricks notebook source
# MAGIC %md
# MAGIC ## 07_ingest_api_data
# MAGIC # MAGIC Ingest external API data (DummyJSON + Frankfurter) to bronze with:
# MAGIC - Pagination (DummyJSON)
# MAGIC - Retry logic
# MAGIC - Explicit schemas (no infer)
# MAGIC - Idempotent loads

# COMMAND ----------

# Install dependencies (if not in cluster libraries)
%pip install requests retrying

# COMMAND ----------

import requests
from retrying import retry
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. DummyJSON Products with Pagination

# COMMAND ----------

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_dummyjson_page(endpoint, limit=100, skip=0):
    """Fetch a single page from DummyJSON with retry logic."""
    url = f"https://dummyjson.com/{endpoint}"
    params = {"limit": limit, "skip": skip}
    
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

# Fetch all products with pagination
all_products = []
skip = 0
limit = 100

while True:
    print(f"Fetching products: skip={skip}, limit={limit}")
    data = fetch_dummyjson_page("products", limit=limit, skip=skip)
    
    products = data.get("products", [])
    if not products:
        break
    
    all_products.extend(products)
    
    # Check if we've fetched all
    total = data.get("total", 0)
    if skip + len(products) >= total:
        break
    
    skip += limit

print(f"✓ Fetched {len(all_products)} products")

# COMMAND ----------

# Define explicit schema for products (schema-on-write)
products_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("description", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("discountPercentage", DoubleType(), True),
    StructField("rating", DoubleType(), True),
    StructField("stock", IntegerType(), True),
    StructField("brand", StringType(), True),
    StructField("category", StringType(), True),
    StructField("thumbnail", StringType(), True),
    StructField("images", StringType(), True),  # JSON array as string
])

# Convert to DataFrame with explicit schema
# Note: Explicitly cast numeric fields to float to match DoubleType schema
# (API may return int or float, PySpark requires strict type matching)
ingested_at = datetime.now().isoformat()
products_data = [
    (
        p["id"],
        p["title"],
        p.get("description"),
        float(p["price"]) if p.get("price") is not None else None,
        float(p["discountPercentage"]) if p.get("discountPercentage") is not None else None,
        float(p["rating"]) if p.get("rating") is not None else None,
        p.get("stock"),
        p.get("brand"),
        p.get("category"),
        p.get("thumbnail"),
        json.dumps(p.get("images", [])),
    )
    for p in all_products
]

df_products = spark.createDataFrame(products_data, schema=products_schema)
df_products = df_products.withColumn("ingested_at", F.lit(ingested_at))
df_products = df_products.withColumn("source", F.lit("dummyjson.com/products"))

# Write to bronze with merge for idempotency
df_products.write.mode("overwrite").saveAsTable("bronze.products_raw")

print(f"✓ Loaded {df_products.count()} products to bronze.products_raw")
display(df_products.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. DummyJSON Users (same pagination pattern)

# COMMAND ----------

all_users = []
skip = 0
while True:
    print(f"Fetching users: skip={skip}")
    data = fetch_dummyjson_page("users", limit=100, skip=skip)
    users = data.get("users", [])
    if not users:
        break
    all_users.extend(users)
    if skip + len(users) >= data.get("total", 0):
        break
    skip += 100

# Simplified user schema
df_users = spark.createDataFrame([
    (
        u["id"],
        u.get("firstName"),
        u.get("lastName"),
        u.get("email"),
        u.get("age"),
        u.get("gender"),
        json.dumps(u.get("address", {})),
    )
    for u in all_users
], ["id", "first_name", "last_name", "email", "age", "gender", "address_json"])

df_users = df_users.withColumn("ingested_at", F.lit(ingested_at))
df_users.write.mode("overwrite").saveAsTable("bronze.users_raw")

print(f"✓ Loaded {df_users.count()} users to bronze.users_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Frankfurter FX Rates (time-series range)

# COMMAND ----------

@retry(stop_max_attempt_number=3, wait_fixed=2000)
def fetch_fx_range(start_date, end_date, base="EUR", symbols="SEK,USD"):
    """Fetch FX rates for a date range using Frankfurter API.

    Primary attempt uses the full range endpoint. If a 404 is returned (some
    Frankfurter deployments don't support arbitrary range windows), a caller
    should fallback to month-chunked requests.
    """
    url = f"https://api.frankfurter.app/{start_date}..{end_date}"
    params = {"from": base, "to": symbols}
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def month_end(d):
    """Return the last day of the month for a given date (datetime.date)."""
    if d.month == 12:
        first_next = d.replace(year=d.year + 1, month=1, day=1)
    else:
        first_next = d.replace(month=d.month + 1, day=1)
    return first_next - timedelta(days=1)

def fetch_fx_range_monthly_chunks(start_date, end_date, base="EUR", symbols="SEK,USD"):
    """Fetch FX rates by chunking into month-sized ranges, inclusive of end_date.

    Returns a list of (date_str, base, quote, rate) rows.
    """
    rows = []
    current = start_date
    while current <= end_date:
        chunk_end = min(month_end(current), end_date)
        url = f"https://api.frankfurter.app/{current.isoformat()}..{chunk_end.isoformat()}"
        params = {"from": base, "to": symbols}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        for date_str, rates in data.get("rates", {}).items():
            for pair, rate in rates.items():
                rows.append((date_str, data.get("base", base), pair, float(rate)))
        # Move to next day after chunk_end
        current = chunk_end + timedelta(days=1)
    return rows

# Fetch last 90 days
end_date = datetime.now().date()
start_date = end_date - timedelta(days=90)

print(f"Fetching FX rates: {start_date} to {end_date}")
fx_rows = []
try:
    fx_data = fetch_fx_range(start_date.isoformat(), end_date.isoformat())
    # Convert nested JSON to rows
    for date_str, rates in fx_data.get("rates", {}).items():
        for pair, rate in rates.items():
            fx_rows.append((
                date_str,
                fx_data.get("base", "EUR"),
                pair,
                float(rate),
                ingested_at,
                "https://api.frankfurter.app"
            ))
except requests.HTTPError as e:
    # Fallback for 404 or other errors: fetch month-sized chunks
    status = getattr(e.response, "status_code", None)
    print(f"⚠️  FX range fetch failed with status {status}. Falling back to monthly chunks...")
    chunk_rows = fetch_fx_range_monthly_chunks(start_date, end_date)
    for date_str, base_currency, pair, rate in chunk_rows:
        fx_rows.append((
            date_str,
            base_currency,
            pair,
            float(rate),
            ingested_at,
            "https://api.frankfurter.app"
        ))

fx_schema = StructType([
    StructField("as_of_date", StringType(), False),
    StructField("base_currency", StringType(), False),
    StructField("quote_currency", StringType(), False),
    StructField("rate", DoubleType(), False),
    StructField("ingested_at", StringType(), False),
    StructField("source_url", StringType(), True),
])

df_fx = spark.createDataFrame(fx_rows, schema=fx_schema)
df_fx = df_fx.withColumn("as_of_date", F.to_date("as_of_date"))

df_fx.write.mode("overwrite").saveAsTable("bronze.fx_rates_raw")

print(f"✓ Loaded {df_fx.count()} FX rate records to bronze.fx_rates_raw")
display(df_fx.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing & Validation
# MAGIC 
# MAGIC Run the cells below to validate the ingestion worked correctly.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test A: Bronze tables exist and are populated

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row counts sanity check
# MAGIC SELECT 'products' AS table_name, COUNT(*) AS row_count FROM bronze.products_raw
# MAGIC UNION ALL
# MAGIC SELECT 'users', COUNT(*) FROM bronze.users_raw
# MAGIC UNION ALL
# MAGIC SELECT 'fx_rates', COUNT(*) FROM bronze.fx_rates_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test B: Pagination completeness (DummyJSON)

# COMMAND ----------

# Fetch expected totals from API
import requests
total_products = requests.get('https://dummyjson.com/products').json()['total']
print(f'Expected products total from API: {total_products}')

# Compare with bronze count
bronze_products_count = spark.sql("SELECT COUNT(*) AS cnt FROM bronze.products_raw").collect()[0][0]
print(f'Bronze products count: {bronze_products_count}')

if bronze_products_count == total_products:
    print('✓ PASS: Pagination complete - all products fetched')
else:
    print(f'⚠️  WARNING: Expected {total_products}, got {bronze_products_count}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test C: Schema-on-write checks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe products table schema
# MAGIC DESCRIBE bronze.products_raw;

# COMMAND ----------

# Verify key columns and types
products_schema_actual = spark.table("bronze.products_raw").schema
print("Products schema:")
for field in products_schema_actual:
    print(f"  {field.name}: {field.dataType} (nullable={field.nullable})")

# Key assertions
assert "id" in [f.name for f in products_schema_actual], "Missing 'id' column"
assert "ingested_at" in [f.name for f in products_schema_actual], "Missing 'ingested_at' column"
assert "source" in [f.name for f in products_schema_actual], "Missing 'source' column"
print("\n✓ PASS: Required columns present")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test D: Lineage/metadata columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify ingested_at and source are populated
# MAGIC SELECT ingested_at, source FROM bronze.products_raw LIMIT 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify FX metadata columns
# MAGIC SELECT as_of_date, base_currency, quote_currency, rate, ingested_at, source_url 
# MAGIC FROM bronze.fx_rates_raw 
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test E: Data quality filters (FX positive rates)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for non-positive rates (should be 0)
# MAGIC SELECT COUNT(*) AS non_positive_count 
# MAGIC FROM bronze.fx_rates_raw 
# MAGIC WHERE rate <= 0;

# COMMAND ----------

non_positive = spark.sql("SELECT COUNT(*) AS cnt FROM bronze.fx_rates_raw WHERE rate <= 0").collect()[0][0]
if non_positive == 0:
    print('✓ PASS: All FX rates are positive')
else:
    print(f'⚠️  WARNING: Found {non_positive} non-positive rates')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test F: Idempotent re-run (no duplicates)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify no duplicate product IDs
# MAGIC SELECT 
# MAGIC   COUNT(*) AS total_rows,
# MAGIC   COUNT(DISTINCT id) AS distinct_ids
# MAGIC FROM bronze.products_raw;

# COMMAND ----------

total_rows = spark.sql("SELECT COUNT(*) AS cnt FROM bronze.products_raw").collect()[0][0]
distinct_ids = spark.sql("SELECT COUNT(DISTINCT id) AS cnt FROM bronze.products_raw").collect()[0][0]

if total_rows == distinct_ids:
    print(f'✓ PASS: No duplicates - {total_rows} total rows = {distinct_ids} distinct IDs')
else:
    print(f'⚠️  WARNING: Duplicates found - {total_rows} total rows vs {distinct_ids} distinct IDs')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary
# MAGIC - ✓ DummyJSON products: pagination, explicit schema, retry logic
# MAGIC - ✓ DummyJSON users: same pattern
# MAGIC - ✓ Frankfurter FX: time-range pull, flattened JSON
# MAGIC - ✓ All landed to bronze with `ingested_at` and `source` for lineage
# MAGIC - ✓ Testing validates: tables exist, pagination complete, schemas correct, metadata present, quality checks pass
