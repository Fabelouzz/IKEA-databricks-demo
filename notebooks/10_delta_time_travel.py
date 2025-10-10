# Databricks notebook source
# MAGIC %md
# MAGIC ## 10_delta_time_travel
# MAGIC 
# MAGIC Delta Lake governance features demonstration:
# MAGIC - DELETE operations (data corrections)
# MAGIC - DESCRIBE HISTORY (audit trail)
# MAGIC - Time travel (versionAsOf, timestampAsOf)
# MAGIC - Rollback patterns (disaster recovery)
# MAGIC 
# MAGIC **Business value**: Auditability, compliance, data recovery, debugging

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Setup: Insert Corrupted Data (Multiple Violation Types)
# MAGIC 
# MAGIC **Enhanced scenario**: Instead of just 1 bad row, we'll insert ~17 corrupted rows across 5 violation types to demonstrate comprehensive data quality governance.

# COMMAND ----------

# PURPOSE: Simulate realistic data quality issues that need governance
# SCENARIOS:
#   1. Orphan foreign keys (customer_id doesn't exist in dimension)
#   2. Duplicate primary keys (same receipt_id)
#   3. Out-of-range dates (1900, 2099)
#   4. Null violations (required fields missing)
#   5. Data type corruption (negative amounts, impossible values)

# Read current gold table (from Step 3)
df_gold = spark.table("gold.baskets_enriched")

# IMPORTANT: Materialize the count BEFORE any modifications
# PySpark DataFrames re-read tables on each action, so we must store the count
original_row_count = df_gold.count()

print(f"📊 Current state (BEFORE corruption):")
print(f"   Rows: {original_row_count:,}")
print(f"   Columns: {len(df_gold.columns)}")

# COMMAND ----------

# Display current schema to see what columns we have
print("Current schema:")
df_gold.printSchema()

# Get a valid receipt_id for duplicate scenario
sample_valid = df_gold.limit(1).collect()[0]
duplicate_receipt_id = sample_valid['receipt_id']

print(f"\n🔍 Will create duplicate of receipt_id: {duplicate_receipt_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Comprehensive Corrupted Dataset
# MAGIC 
# MAGIC This demonstrates real-world data quality scenarios:
# MAGIC - Orphan FKs: ETL bug dropped dimension load
# MAGIC - Duplicates: Retry logic error, no dedup
# MAGIC - Bad dates: Legacy system migration error
# MAGIC - Nulls: Schema mismatch, missing required fields
# MAGIC - Impossible values: Data entry error, unit conversion bug

# COMMAND ----------

# Import date function for proper type conversion
from datetime import date as dt_date

# Create comprehensive corrupted dataset
corrupted_rows = []

# Scenario 1: ORPHAN FOREIGN KEYS (5 rows)
# customer_id values that don't exist in dim_customers_api
print("Creating Scenario 1: Orphan Foreign Keys (5 rows)...")
for i in range(5):
    corrupted_rows.append((
        900001 + i,              # receipt_id (unique invalid IDs)
        -9990 - i,               # loyalty_id (orphan - doesn't exist)
        dt_date(2025, 1, 10),    # date (valid)
        1,                       # store_id (valid)
        1,                       # attached
        -9990 - i,               # customer_id (ORPHAN - no match in dimension)
        f"ORPHAN_CUSTOMER_{i}",  # customer_name (flagged)
        None,                    # age
        "unknown",               # gender
        None,                    # email
        None,                    # as_of_date
        None,                    # pair
        None                     # rate
    ))

# Scenario 2: DUPLICATE PRIMARY KEYS (3 rows)
# Same receipt_id inserted multiple times
print("Creating Scenario 2: Duplicate Primary Keys (3 rows)...")
for i in range(3):
    corrupted_rows.append((
        duplicate_receipt_id,    # DUPLICATE receipt_id (PK violation!)
        1,                       # loyalty_id
        dt_date(2025, 1, 15),    # date
        2,                       # store_id
        0,                       # attached
        1,                       # customer_id
        f"DUPLICATE_ROW_{i}",    # customer_name (to distinguish copies)
        25 + i,                  # age
        "male",                  # gender
        f"dup{i}@test.com",      # email
        None,                    # as_of_date
        None,                    # pair
        None                     # rate
    ))

# Scenario 3: OUT-OF-RANGE DATES (3 rows)
# Dates from 1900 or 2099 (clearly invalid)
print("Creating Scenario 3: Out-of-Range Dates (3 rows)...")
corrupted_rows.append((
    900010,                      # receipt_id
    1,                           # loyalty_id
    dt_date(1900, 1, 1),        # BAD DATE: Too old (legacy data bug)
    1,                           # store_id
    0,                           # attached
    1,                           # customer_id
    "OLD_DATE_ERROR",            # customer_name
    None, "unknown", None, None, None, None
))
corrupted_rows.append((
    900011,
    1,
    dt_date(2099, 12, 31),      # BAD DATE: Future (clock skew, test data)
    1, 0, 1,
    "FUTURE_DATE_ERROR",
    None, "unknown", None, None, None, None
))
corrupted_rows.append((
    900012,
    1,
    dt_date(1970, 1, 1),        # BAD DATE: Unix epoch (default value bug)
    1, 0, 1,
    "EPOCH_DATE_ERROR",
    None, "unknown", None, None, None, None
))

# Scenario 4: NULL VIOLATIONS (3 rows)
# Required fields set to None
print("Creating Scenario 4: Null Violations (3 rows)...")
corrupted_rows.append((
    None,                        # NULL receipt_id (PK violation!)
    1,
    dt_date(2025, 1, 20),
    1, 0, 1,
    "NULL_RECEIPT_ID",
    None, "unknown", None, None, None, None
))
corrupted_rows.append((
    900014,
    None,                        # NULL loyalty_id (FK field)
    dt_date(2025, 1, 20),
    1, 0,
    None,                        # NULL customer_id (FK violation)
    "NULL_FK_ERROR",
    None, "unknown", None, None, None, None
))
corrupted_rows.append((
    900015,
    1,
    None,                        # NULL date (required field)
    1, 0, 1,
    "NULL_DATE_ERROR",
    None, "unknown", None, None, None, None
))

# Scenario 5: DATA TYPE CORRUPTION (3 rows)
# Negative store_ids, impossible ages, corrupted names
print("Creating Scenario 5: Data Type Corruption (3 rows)...")
corrupted_rows.append((
    900016,
    1,
    dt_date(2025, 1, 25),
    -999,                        # NEGATIVE store_id (impossible)
    1, 1,
    "NEGATIVE_STORE_ID",
    None, "unknown", None, None, None, None
))
corrupted_rows.append((
    900017,
    1,
    dt_date(2025, 1, 25),
    1, 1, 1,
    "###CORRUPTED###",           # Corrupted name (special chars, encoding issue)
    999,                         # Impossible age
    "INVALID_GENDER_123",        # Invalid gender value
    "not_an_email",              # Malformed email
    None, None, None
))
corrupted_rows.append((
    900018,
    1,
    dt_date(2025, 1, 25),
    999999,                      # Non-existent store_id (referential integrity)
    1, 1,
    "",                          # EMPTY string (not null but invalid)
    -10,                         # Negative age
    "",                          # Empty gender
    None, None, None, None
))

# Convert to DataFrame
df_corrupted = spark.createDataFrame(corrupted_rows, schema=df_gold.schema)

print(f"\n🔴 Created corrupted dataset:")
print(f"   Total corrupted rows: {df_corrupted.count()}")
print(f"   Violation types: 5")
print("\nSample corrupted rows:")
df_corrupted.select("receipt_id", "loyalty_id", "date", "store_id", "customer_name").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Insert Corrupted Data + Catalog Violations

# COMMAND ----------

# Append corrupted rows to the gold table
# MODE: append (adds to existing data without removing anything)
df_corrupted.write.mode("append").saveAsTable("gold.baskets_enriched")

print("✓ Corrupted data inserted to gold.baskets_enriched")

# COMMAND ----------

# Verify corrupted rows exist
new_count = spark.table("gold.baskets_enriched").count()
corrupted_count = df_corrupted.count()

print(f"📊 After corruption insert:")
print(f"   Before: {original_row_count:,} rows")
print(f"   After: {new_count:,} rows")
print(f"   Corrupted rows added: +{corrupted_count}")
print(f"   Expected difference: {new_count - original_row_count} (should match corrupted count)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Data Quality Violations Catalog
# MAGIC 
# MAGIC This table tracks detected corruption for audit and analysis.
# MAGIC In production, this would feed into a data observability dashboard.

# COMMAND ----------

# Create schema for ops if it doesn't exist
spark.sql("CREATE SCHEMA IF NOT EXISTS ops")

# Build violations catalog with metadata
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

violations_data = []

# Scenario 1: Orphan FKs
for i in range(5):
    violations_data.append((
        900001 + i,
        "ORPHAN_FK",
        "HIGH",
        "customer_id doesn't exist in dim_customers_api",
        datetime.datetime.now()
    ))

# Scenario 2: Duplicate PKs
for i in range(3):
    violations_data.append((
        duplicate_receipt_id,
        "DUPLICATE_PK",
        "CRITICAL",
        "receipt_id appears multiple times (PK uniqueness violation)",
        datetime.datetime.now()
    ))

# Scenario 3: Out-of-range dates
for receipt_id, desc in [(900010, "Date in 1900"), (900011, "Date in 2099"), (900012, "Unix epoch date")]:
    violations_data.append((
        receipt_id,
        "INVALID_DATE",
        "HIGH",
        desc,
        datetime.datetime.now()
    ))

# Scenario 4: Null violations
for receipt_id, desc in [(None, "Null receipt_id (PK)"), (900014, "Null FK fields"), (900015, "Null date")]:
    violations_data.append((
        receipt_id,
        "NULL_VIOLATION",
        "CRITICAL",
        desc,
        datetime.datetime.now()
    ))

# Scenario 5: Data type corruption
for receipt_id, desc in [(900016, "Negative store_id"), (900017, "Corrupted fields"), (900018, "Empty required fields")]:
    violations_data.append((
        receipt_id,
        "DATA_CORRUPTION",
        "MEDIUM",
        desc,
        datetime.datetime.now()
    ))

df_violations = spark.createDataFrame(violations_data, 
    ["receipt_id", "violation_type", "severity", "description", "detected_at"])

# Write to ops.data_quality_violations table
df_violations.write.mode("overwrite").saveAsTable("ops.data_quality_violations")

print("✓ Created ops.data_quality_violations catalog")
print(f"   Total violations cataloged: {df_violations.count()}")
print("\nViolations by type:")
df_violations.groupBy("violation_type", "severity").count().orderBy("violation_type").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query Corrupted Rows in Gold Table

# COMMAND ----------

# Query to find corrupted rows
print("🔍 Locating corrupted rows in gold.baskets_enriched:")
print("\n1. Orphan foreign keys:")
spark.sql("""
  SELECT receipt_id, customer_id, customer_name, loyalty_id
  FROM gold.baskets_enriched 
  WHERE receipt_id BETWEEN 900001 AND 900005
""").show(5, truncate=False)

print("\n2. Duplicate primary keys:")
spark.sql(f"""
  SELECT receipt_id, customer_name, date, store_id
  FROM gold.baskets_enriched 
  WHERE receipt_id = {duplicate_receipt_id}
  ORDER BY customer_name
""").show(10, truncate=False)

print("\n3. Out-of-range dates:")
spark.sql("""
  SELECT receipt_id, date, customer_name
  FROM gold.baskets_enriched 
  WHERE receipt_id IN (900010, 900011, 900012)
  ORDER BY receipt_id
""").show(5, truncate=False)

print("\n4. Data type corruption:")
spark.sql("""
  SELECT receipt_id, store_id, customer_name, age, gender
  FROM gold.baskets_enriched 
  WHERE receipt_id IN (900016, 900017, 900018)
  ORDER BY receipt_id
""").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. DELETE Operations (Data Corrections)
# MAGIC 
# MAGIC **Governance demonstration**: Use Delta DELETE to correct multiple data quality issues.
# MAGIC We'll delete corrupted rows using different WHERE clause patterns to showcase surgical data removal.

# COMMAND ----------

# PURPOSE: Remove the corrupted data across 5 violation types
# PATTERN: Delta Lake supports SQL DELETE (ACID transaction)
# BENEFIT: Surgical data removal without rewriting entire table

# Load table as Delta table object
delta_table = DeltaTable.forName(spark, "gold.baskets_enriched")

# Count before deletion
count_before_delete = spark.table("gold.baskets_enriched").count()
print(f"📊 Before DELETE operations:")
print(f"   Total rows: {count_before_delete:,}")
print(f"   Original clean rows: {original_row_count:,}")
print(f"   Corrupted rows to remove: {corrupted_count}")
print("="*70)

# COMMAND ----------

# MAGIC %md
# MAGIC #### DELETE 1: Remove Orphan Foreign Keys

# COMMAND ----------

# Delete rows where customer_id doesn't exist in dimension (orphan FKs)
# In production, this would be detected by an anti-join or quality check

delta_table.delete("receipt_id BETWEEN 900001 AND 900005")

count_after_1 = spark.table("gold.baskets_enriched").count()
print("✓ DELETE 1 complete: Removed orphan FK rows")
print(f"   Condition: receipt_id BETWEEN 900001 AND 900005")
print(f"   Rows after: {count_after_1:,} (deleted ~5)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DELETE 2: Remove Duplicate Primary Keys

# COMMAND ----------

# Delete duplicate receipt_id rows (keep original, remove duplicates)
# We identify duplicates by the DUPLICATE_ROW_* customer_name pattern

delta_table.delete(f"receipt_id = {duplicate_receipt_id} AND customer_name LIKE 'DUPLICATE_ROW_%'")

count_after_2 = spark.table("gold.baskets_enriched").count()
print("✓ DELETE 2 complete: Removed duplicate PK rows")
print(f"   Condition: receipt_id = {duplicate_receipt_id} AND customer_name LIKE 'DUPLICATE_ROW_%'")
print(f"   Rows after: {count_after_2:,} (deleted ~3)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DELETE 3: Remove Out-of-Range Dates

# COMMAND ----------

# Delete rows with dates outside acceptable range (2020-2026)
delta_table.delete("date < '2020-01-01' OR date > '2026-12-31'")

count_after_3 = spark.table("gold.baskets_enriched").count()
print("✓ DELETE 3 complete: Removed out-of-range dates")
print("   Condition: date < '2020-01-01' OR date > '2026-12-31'")
print(f"   Rows after: {count_after_3:,} (deleted ~3)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DELETE 4: Remove Null Violations

# COMMAND ----------

# Delete rows with null in required fields
delta_table.delete("receipt_id IS NULL OR date IS NULL OR loyalty_id IS NULL")

count_after_4 = spark.table("gold.baskets_enriched").count()
print("✓ DELETE 4 complete: Removed null violations")
print("   Condition: receipt_id IS NULL OR date IS NULL OR loyalty_id IS NULL")
print(f"   Rows after: {count_after_4:,} (deleted ~3)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DELETE 5: Remove Data Type Corruption

# COMMAND ----------

# Delete rows with impossible values
delta_table.delete("""
  store_id < 0 
  OR age < 0 
  OR age > 120 
  OR customer_name = '' 
  OR customer_name LIKE '%###%'
  OR store_id > 10000
""")

count_after_5 = spark.table("gold.baskets_enriched").count()
print("✓ DELETE 5 complete: Removed data type corruption")
print("   Condition: Negative/impossible values, empty strings, corrupted names")
print(f"   Rows after: {count_after_5:,} (deleted ~3)")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DELETE Summary & Verification

# COMMAND ----------

final_count = spark.table("gold.baskets_enriched").count()
total_deleted = count_before_delete - final_count

print("=" * 70)
print("DELETE OPERATIONS SUMMARY")
print("=" * 70)
print(f"Before corruption: {original_row_count:,} rows")
print(f"After insert: {count_before_delete:,} rows (+{corrupted_count} corrupted)")
print(f"After 5 DELETE operations: {final_count:,} rows")
print(f"Total deleted: {total_deleted} rows")
print(f"Back to clean state: {final_count == original_row_count}")
print("=" * 70)

# Verify no corrupted rows remain
corrupted_remaining = spark.sql("""
  SELECT COUNT(*) AS cnt
  FROM gold.baskets_enriched
  WHERE receipt_id >= 900000
     OR customer_name LIKE '%ORPHAN%'
     OR customer_name LIKE '%DUPLICATE%'
     OR customer_name LIKE '%ERROR%'
     OR customer_name LIKE '%CORRUPT%'
""").collect()[0][0]

print(f"\n✅ Final Verification:")
print(f"   Corrupted rows remaining: {corrupted_remaining} (should be 0)")
print(f"   Status: {'PASS ✓' if corrupted_remaining == 0 and final_count == original_row_count else 'FAIL ✗'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. DESCRIBE HISTORY (Audit Trail)

# COMMAND ----------

# PURPOSE: Show complete history of all changes to the table
# BENEFIT: Auditability, compliance, debugging, forensics
# USE CASES: "Who changed what, when?" "What was the data 2 hours ago?"

print("📜 DELTA TABLE HISTORY")
print("="*60)
print("Every change to this table is tracked automatically.\n")

# Get history DataFrame
history_df = delta_table.history()

# Display key fields (version, timestamp, operation, user, etc.)
display(history_df.select(
    "version",           # Version number (increments with each write)
    "timestamp",         # When the operation occurred
    "operation",         # Type: WRITE, DELETE, UPDATE, MERGE, etc.
    "operationParameters",  # Details: predicate, mode, etc.
    "operationMetrics"      # Stats: numFiles, numOutputRows, etc.
))

# COMMAND ----------

# MAGIC %md
# MAGIC 📸 **SCREENSHOT THIS OUTPUT** for documentation!
# MAGIC 
# MAGIC Save as: `docs/screenshots/delta_history.png`
# MAGIC 
# MAGIC **What to look for:**
# MAGIC - Version numbers incrementing (0, 1, 2, ...)
# MAGIC - Operations: WRITE (insert), DELETE (our correction)
# MAGIC - Timestamps showing when each change occurred
# MAGIC - Metrics showing rows affected

# COMMAND ----------

# Print detailed history in text format
print("\n📋 DETAILED HISTORY:")
print("="*60)

for row in history_df.collect():
    print(f"\nVersion {row['version']}:")
    print(f"  Timestamp: {row['timestamp']}")
    print(f"  Operation: {row['operation']}")
    print(f"  User: {row['userName'] if 'userName' in row else 'N/A'}")
    if row['operationMetrics']:
        print(f"  Metrics: {row['operationMetrics']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Time Travel: Read Previous Versions

# COMMAND ----------

# PURPOSE: Access historical data (before DELETE)
# PATTERN: Read specific versions using versionAsOf
# USE CASES: Rollback, auditing, comparing changes, debugging

# Get version numbers from history
versions = history_df.select("version").rdd.flatMap(lambda x: x).collect()
latest_version = max(versions)
if len(versions) > 1:
    previous_version = latest_version - 1
else:
    previous_version = 0

print(f"📊 VERSION INFO:")
print(f"   Latest version: {latest_version}")
print(f"   Previous version: {previous_version}")

# COMMAND ----------

# Read the version BEFORE the delete (when bad row still existed)
# SYNTAX: .option("versionAsOf", version_number)

if previous_version < latest_version:
    print(f"\n🕰️  TIME TRAVEL: Reading version {previous_version}")
    print("="*60)
    
    df_v_before_delete = (spark.read
                          .format("delta")
                          .option("versionAsOf", previous_version)
                          .table("gold.baskets_enriched"))
    
    print(f"Version {previous_version} (before delete) had {df_v_before_delete.count():,} rows")
    
    # Verify bad row exists in old version
    print(f"\n🔍 Looking for bad row in version {previous_version}:")
    df_v_before_delete.filter("receipt_id = 999999").show(truncate=False)
else:
    print("⚠️  Only one version exists, skipping time travel demo")

# COMMAND ----------

# Read current version (after delete)
df_current = spark.table("gold.baskets_enriched")
print(f"\n📊 CURRENT VERSION:")
print(f"   Rows: {df_current.count():,}")

# Verify bad row is gone in current version
bad_count_current = df_current.filter("receipt_id = 999999").count()
print(f"   Bad rows: {bad_count_current}")
print(f"   Status: {'✓ Clean' if bad_count_current == 0 else '✗ Still has bad data'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Rollback Patterns (Disaster Recovery)

# COMMAND ----------

# PURPOSE: Show how to recover from mistakes
# SCENARIOS:
# - Accidentally deleted important data
# - ETL bug corrupted the table
# - Need to revert to yesterday's state

print("🔄 ROLLBACK OPTIONS")
print("="*60)
print("""
Three ways to rollback a Delta table:

1. RESTORE command (Databricks SQL):
   RESTORE TABLE gold.baskets_enriched TO VERSION AS OF {version}
   
2. Overwrite with old version (PySpark):
   df_old = spark.read.format("delta").option("versionAsOf", X).table("...")
   df_old.write.mode("overwrite").saveAsTable("...")
   
3. Timestamp-based restore:
   RESTORE TABLE gold.baskets_enriched TO TIMESTAMP AS OF '2025-10-16 14:30:00'

Production Best Practices:
- Always test in dev/staging first
- Communicate to team before rollback
- Document why rollback was needed
- Consider impact on downstream consumers
""")

# COMMAND ----------

# Example: How to rollback (commented out to prevent accidental execution)

# Option 1: SQL RESTORE (Databricks SQL)
# spark.sql(f"RESTORE TABLE gold.baskets_enriched TO VERSION AS OF {previous_version}")

# Option 2: Overwrite with old version
# df_old_version = spark.read.format("delta").option("versionAsOf", previous_version).table("gold.baskets_enriched")
# df_old_version.write.mode("overwrite").option("overwriteSchema", "false").saveAsTable("gold.baskets_enriched")

print("\n✓ Rollback examples documented (not executed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Timestamp-Based Time Travel

# COMMAND ----------

# PURPOSE: Access data as it existed at a specific time
# PATTERN: Use timestampAsOf instead of versionAsOf
# USE CASE: "Show me the data as it was at 2pm yesterday"

# Try to read data from 2 hours ago
two_hours_ago = (datetime.datetime.now() - datetime.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")

print(f"🕰️  TIMESTAMP-BASED TIME TRAVEL")
print(f"   Looking for data as of: {two_hours_ago}")

try:
    df_ts = (spark.read
             .format("delta")
             .option("timestampAsOf", two_hours_ago)
             .table("gold.baskets_enriched"))
    
    print(f"   ✓ Found data from {two_hours_ago}")
    print(f"   Rows at that time: {df_ts.count():,}")
except Exception as e:
    print(f"   ⚠️  No data at {two_hours_ago} (table created recently)")
    print(f"   This is expected for a newly created demo table")

# COMMAND ----------

# Show all available timestamps
print("\n📅 AVAILABLE VERSIONS AND TIMESTAMPS:")
print("="*60)

history_df.select("version", "timestamp").orderBy("version").show(truncate=False)

print("\nYou can time travel to any of these timestamps!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Summary & Key Takeaways

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════╗
║         DELTA LAKE TIME TRAVEL - SUMMARY                     ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║ ✓ DELETE operation executed (removed corrupted data)        ║
║ ✓ DESCRIBE HISTORY shows full audit trail                   ║
║ ✓ Time travel demonstrated (read old versions)              ║
║ ✓ Rollback patterns documented                              ║
║ ✓ Timestamp-based queries shown                             ║
║                                                              ║
║ SKILLS DEMONSTRATED:                                         ║
║ • Delta operations (DELETE, not just append/overwrite)       ║
║ • Audit trail & governance (every change tracked)            ║
║ • Time travel for recovery & debugging                       ║
║ • Rollback strategies for production incidents               ║
║ • ACID transactions (atomic deletes)                         ║
║                                                              ║
║ BUSINESS VALUE:                                              ║
║ • Compliance: Full audit trail of all changes                ║
║ • Recovery: Rollback from mistakes in minutes                ║
║ • Debugging: See exactly what changed and when               ║
║ • Confidence: Can't permanently lose data                    ║
║                                                              ║
║ PRODUCTION TIPS:                                             ║
║ • Set retention period (default 30 days)                     ║
║ • Use VACUUM to clean up old files (after retention)         ║
║ • Monitor storage for version history growth                 ║
║ • Document rollback procedures in runbooks                   ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# Final verification
print("\n✅ FINAL STATE CHECK:")
print("="*60)

final_count = spark.table("gold.baskets_enriched").count()
history_count = delta_table.history().count()

print(f"Table: gold.baskets_enriched")
print(f"  Current rows: {final_count:,}")
print(f"  Version history entries: {history_count}")
print(f"  Latest version: {latest_version}")
print(f"\nAll operations completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Schema Evolution: Column Operations
# MAGIC 
# MAGIC Demonstrate Delta Lake's schema evolution capabilities:
# MAGIC - ADD COLUMN with default values
# MAGIC - RENAME COLUMN 
# MAGIC - DROP COLUMN
# MAGIC - Schema compatibility with time travel

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8.1 Add New Column with Default Value

# COMMAND ----------

# Show current schema before changes
print("📋 CURRENT SCHEMA (before changes):")
print("="*60)
current_schema = spark.table("gold.baskets_enriched").schema
for field in current_schema:
    print(f"  {field.name}: {field.dataType}")
print(f"\nTotal columns: {len(current_schema)}")

# COMMAND ----------

# Add a new column with default value
# USE CASE: Add audit field to track data quality
# NOTE: Delta Lake requires 2 steps for existing tables:
#   1. ADD COLUMN (without DEFAULT)
#   2. UPDATE existing rows + SET DEFAULT for future inserts

# Step 1: Add column (nullable, will be NULL for existing rows)
spark.sql("""
  ALTER TABLE gold.baskets_enriched 
  ADD COLUMN data_quality_score DOUBLE
  COMMENT 'Data quality score (0.0-1.0, default is perfect quality)'
""")

print("✓ Step 1: Added column: data_quality_score (nullable)")

# Step 2: Set default for future inserts
spark.sql("""
  ALTER TABLE gold.baskets_enriched 
  ALTER COLUMN data_quality_score SET DEFAULT 1.0
""")

print("✓ Step 2: Set DEFAULT 1.0 for future inserts")

# Step 3: Backfill existing rows (UPDATE)
spark.sql("""
  UPDATE gold.baskets_enriched
  SET data_quality_score = 1.0
  WHERE data_quality_score IS NULL
""")

print("✓ Step 3: Backfilled existing rows with 1.0")

# Verify the new column exists and is populated
df_after_add = spark.table("gold.baskets_enriched")
print(f"\n📊 After ADD COLUMN:")
print(f"  Total columns: {len(df_after_add.columns)}")
print(f"  New column exists: {'data_quality_score' in df_after_add.columns}")

# Check for nulls (should be 0 after backfill)
null_count = df_after_add.filter("data_quality_score IS NULL").count()
print(f"  Null values: {null_count} (should be 0)")

# Show sample values (should all be 1.0 for existing rows)
print("\nSample values:")
df_after_add.select("receipt_id", "customer_name", "data_quality_score").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8.2 Rename Column

# COMMAND ----------

# Enable column mapping for rename support
# NOTE: This is required for Delta Lake to support RENAME COLUMN
spark.sql("""
  ALTER TABLE gold.baskets_enriched 
  SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
""")

print("✓ Enabled column mapping mode")

# COMMAND ----------

# Rename a column for clarity
# USE CASE: Improve column naming convention
spark.sql("""
  ALTER TABLE gold.baskets_enriched 
  RENAME COLUMN customer_name TO full_name
""")

print("✓ Renamed column: customer_name → full_name")

# Verify the rename
df_after_rename = spark.table("gold.baskets_enriched")
print(f"\n📊 After RENAME COLUMN:")
print(f"  'customer_name' exists: {'customer_name' in df_after_rename.columns}")
print(f"  'full_name' exists: {'full_name' in df_after_rename.columns}")

# Show sample
print("\nSample with new column name:")
df_after_rename.select("receipt_id", "full_name", "data_quality_score").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8.3 Drop Column

# COMMAND ----------

# Drop a column that's no longer needed
# USE CASE: Remove deprecated or sensitive columns
spark.sql("""
  ALTER TABLE gold.baskets_enriched 
  DROP COLUMN pair
""")

print("✓ Dropped column: pair")

# Verify the drop
df_after_drop = spark.table("gold.baskets_enriched")
print(f"\n📊 After DROP COLUMN:")
print(f"  Total columns: {len(df_after_drop.columns)}")
print(f"  'pair' exists: {'pair' in df_after_drop.columns}")

# Show final schema
print("\n📋 FINAL SCHEMA (after all changes):")
for field in df_after_drop.schema:
    print(f"  {field.name}: {field.dataType}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8.4 Time Travel Still Works After Schema Changes

# COMMAND ----------

# Key insight: Old versions retain their original schema
# You can still read data from before the schema changes

print("🕰️ TIME TRAVEL WITH SCHEMA EVOLUTION")
print("="*60)

# Read version 0 (original schema - has 'pair' column, no 'data_quality_score')
df_v0 = spark.read.format("delta").option("versionAsOf", 0).table("gold.baskets_enriched")
print(f"\n📊 Version 0 (original):")
print(f"  Columns: {len(df_v0.columns)}")
print(f"  Has 'pair': {'pair' in df_v0.columns}")
print(f"  Has 'data_quality_score': {'data_quality_score' in df_v0.columns}")
print(f"  Has 'customer_name': {'customer_name' in df_v0.columns}")
print(f"  Has 'full_name': {'full_name' in df_v0.columns}")

# Current version (modified schema)
df_current = spark.table("gold.baskets_enriched")
print(f"\n📊 Current version:")
print(f"  Columns: {len(df_current.columns)}")
print(f"  Has 'pair': {'pair' in df_current.columns}")
print(f"  Has 'data_quality_score': {'data_quality_score' in df_current.columns}")
print(f"  Has 'customer_name': {'customer_name' in df_current.columns}")
print(f"  Has 'full_name': {'full_name' in df_current.columns}")

print("\n✓ Time travel works! Each version has its own schema.")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 8.5 Schema Evolution Summary

# COMMAND ----------

# Show complete history including schema changes
print("\n📜 COMPLETE HISTORY (including schema changes):")
print("="*60)

history_with_schema = delta_table.history()
history_with_schema.select(
    "version", 
    "timestamp", 
    "operation",
    "operationParameters"
).show(20, truncate=False)

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    SCHEMA EVOLUTION - KEY TAKEAWAYS                          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║ ✅ ADD COLUMN: Fast, metadata-only operation                                ║
║    → For existing tables: 3 steps (ADD → SET DEFAULT → UPDATE)              ║
║    → DEFAULT only applies to future inserts, must UPDATE existing rows      ║
║ ✅ RENAME COLUMN: Requires column mapping mode                              ║
║    → Must enable 'delta.columnMapping.mode' = 'name' first                  ║
║ ✅ DROP COLUMN: Metadata-only, data still in Parquet files                  ║
║    → Physical data deleted only after VACUUM                                ║
║ ✅ TIME TRAVEL: Each version retains its original schema                    ║
║    → Can query old versions with dropped columns                            ║
║ ✅ BACKWARD COMPATIBLE: Old queries still work (if columns exist)           ║
║                                                                              ║
║ IMPORTANT CONSTRAINTS:                                                       ║
║ • Can't add column with DEFAULT in one step for existing tables             ║
║ • Dropped columns can't be restored (but time travel can read them)         ║
║ • Column mapping mode can't be disabled once enabled                        ║
║ • Storage not reclaimed until VACUUM (past retention period)                ║
║ • UPDATE is required to backfill existing rows with default values          ║
║                                                                              ║
║ INTERVIEW ANSWER:                                                            ║
║ "Yes, Delta Lake supports ADD, RENAME, and DROP columns. ADD/DROP are       ║
║  metadata-only operations. For ADD with DEFAULT on existing tables, you     ║
║  need 3 steps: ADD COLUMN, SET DEFAULT, then UPDATE existing rows.          ║
║  For RENAME, you must enable column mapping mode first. Time travel         ║
║  still works - each version retains its original schema."                   ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

