# Step 4: Delta Time Travel & Corrections - Quick Start Guide

## What Was Implemented

### File Created
**`notebooks/10_delta_time_travel.py`** - Delta governance demonstration

### Delta Features Demonstrated
1. **DELETE operations** - Surgical data removal
2. **DESCRIBE HISTORY** - Complete audit trail
3. **Time travel** - Read previous versions
4. **Rollback patterns** - Disaster recovery
5. **Timestamp queries** - Point-in-time data access
6. **Schema evolution** - ADD, RENAME, DROP columns

## Prerequisites

**Step 3 must be complete:**
- ✅ `gold.baskets_enriched` table exists (Delta format)

## How to Run on Databricks

### Step-by-Step Execution

#### 1. Open the Notebook
- Navigate to `notebooks/10_delta_time_travel.py`
- Attach to your cluster (same as previous steps)

#### 2. Run All Cells
Execute cells in order (or "Run All"):

**Section 1: Setup (cells 1-7)**
- Check current table state
- Insert a "bad" row (simulates data corruption)
- Verify bad row exists

**Section 2: DELETE (cells 8-10)**
- Delete the corrupted row using Delta DELETE
- Verify deletion successful

**Section 3: History (cells 11-13)**
- Display complete change history
- **📸 Screenshot this for documentation!**

**Section 4: Time Travel (cells 14-16)**
- Read previous version (before delete)
- Verify bad row exists in old version
- Confirm current version is clean

**Section 5: Rollback (cells 17-18)**
- Document rollback patterns
- Show 3 different recovery methods

**Section 6: Timestamps (cells 19-20)**
- Demonstrate timestamp-based queries
- Show all available versions

**Section 7: Summary (cell 21)**
- Final verification

**Section 8: Schema Evolution (cells 22-29)**
- ADD COLUMN with default value
- Enable column mapping mode
- RENAME COLUMN
- DROP COLUMN
- Time travel with schema changes
- Schema evolution summary

### Expected Output

#### After Section 1 (Insert Bad Row):
```
📊 Current state:
   Rows: XXX
   Columns: 13

🔴 Inserting bad row (simulating data corruption)...

📊 After bad insert:
   Rows: XXX + 1
   Difference: +1

🔍 Locating bad row:
receipt_id | loyalty_id | customer_name    | store_id | attached
999999     | -1         | CORRUPTED DATA   | -999     | 0
```

#### After Section 2 (DELETE):
```
🗑️  DELETING BAD ROW
✓ Deleted row where receipt_id = 999999

📊 After delete:
   Rows: XXX (back to original)
   Matches original: True

✅ Verification:
   Bad rows remaining: 0
   Status: PASS
```

#### Section 3 (HISTORY) - Most Important!
```
📜 DELTA TABLE HISTORY
============================================================

version | timestamp           | operation      | operationParameters
0       | 2024-10-16 14:23... | WRITE          | {mode: Overwrite, ...}
1       | 2024-10-16 14:45... | WRITE          | {mode: Append, ...}
2       | 2024-10-16 14:46... | DELETE         | {predicate: ["(receipt_id = 999999)"], ...}
3       | 2024-10-16 14:47... | ADD COLUMNS    | {columns: ["data_quality_score"], ...}
4       | 2024-10-16 14:48... | RENAME COLUMN  | {oldColumnName: "customer_name", ...}
5       | 2024-10-16 14:49... | DROP COLUMNS   | {columns: ["pair"], ...}
```

#### Section 8 (Schema Evolution):
```
📋 CURRENT SCHEMA (before changes):
  receipt_id: IntegerType
  customer_name: StringType
  ...
  pair: StringType
  Total columns: 13

✓ Added column: data_quality_score (default: 1.0)
✓ Enabled column mapping mode
✓ Renamed column: customer_name → full_name
✓ Dropped column: pair

📋 FINAL SCHEMA (after all changes):
  receipt_id: IntegerType
  full_name: StringType  ← renamed
  ...
  data_quality_score: DoubleType  ← new
  (pair column removed)
  Total columns: 13

🕰️ TIME TRAVEL WITH SCHEMA EVOLUTION:
  Version 0 has: 'customer_name', 'pair', no 'data_quality_score'
  Current has: 'full_name', 'data_quality_score', no 'pair'
  ✓ Time travel works! Each version has its own schema.
```

## What This Proves (Interview Skills)

| Skill | Evidence |
|-------|----------|
| **Delta operations** | DELETE command (not just append/overwrite) |
| **Schema evolution** | ADD, RENAME, DROP columns with backward compatibility |
| **Audit trail** | Every change tracked with user, timestamp, metrics |
| **Time travel** | Read any previous version by number or timestamp |
| **Recovery** | Documented 3 rollback methods |
| **ACID transactions** | Atomic delete (all-or-nothing) |
| **Governance** | Compliance-ready audit log |

## Key Concepts Explained

### 1. DESCRIBE HISTORY
Shows every operation performed on the table:
- **version**: Incrementing number (0, 1, 2, ...)
- **timestamp**: When operation occurred
- **operation**: WRITE, DELETE, UPDATE, MERGE, etc.
- **operationParameters**: Details (e.g., DELETE predicate)
- **operationMetrics**: Stats (rows affected, files changed)

### 2. Time Travel Syntax

**By version number:**
```python
df = (spark.read
      .format("delta")
      .option("versionAsOf", 1)
      .table("gold.baskets_enriched"))
```

**By timestamp:**
```python
df = (spark.read
      .format("delta")
      .option("timestampAsOf", "2025-10-16 14:30:00")
      .table("gold.baskets_enriched"))
```

### 3. Rollback Methods

**Method 1: RESTORE (Databricks SQL)**
```sql
RESTORE TABLE gold.baskets_enriched TO VERSION AS OF 1;
```

**Method 2: Overwrite with old version (PySpark)**
```python
df_old = spark.read.format("delta").option("versionAsOf", 1).table("...")
df_old.write.mode("overwrite").saveAsTable("...")
```

**Method 3: Timestamp-based restore**
```sql
RESTORE TABLE gold.baskets_enriched 
TO TIMESTAMP AS OF '2025-10-16 14:00:00';
```

### 4. Schema Evolution

**ADD COLUMN with default value:**
```sql
ALTER TABLE gold.baskets_enriched 
ADD COLUMN data_quality_score DOUBLE DEFAULT 1.0;
```
- Metadata-only operation (fast)
- Default value applied to existing rows
- No data rewrite needed

**RENAME COLUMN (requires column mapping):**
```sql
-- Enable column mapping first
ALTER TABLE gold.baskets_enriched 
SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name');

-- Then rename
ALTER TABLE gold.baskets_enriched 
RENAME COLUMN customer_name TO full_name;
```
- Can't be disabled once enabled
- Allows flexible schema changes

**DROP COLUMN:**
```sql
ALTER TABLE gold.baskets_enriched 
DROP COLUMN pair;
```
- Metadata-only (data still in Parquet files)
- Can't be restored (but time travel can read old versions)
- Storage reclaimed with VACUUM

## Validation Tests

### Test 1: Verify DELETE worked
```sql
SELECT COUNT(*) FROM gold.baskets_enriched WHERE receipt_id = 999999;
-- Expected: 0
```

### Test 2: Check history exists
```python
delta_table = DeltaTable.forName(spark, "gold.baskets_enriched")
history_count = delta_table.history().count()
print(f"History entries: {history_count}")
-- Expected: At least 3 (initial write, append bad row, delete)
```

### Test 3: Time travel works
```python
df_v0 = spark.read.format("delta").option("versionAsOf", 0).table("gold.baskets_enriched")
df_current = spark.table("gold.baskets_enriched")
print(f"Version 0: {df_v0.count()} rows")
print(f"Current: {df_current.count()} rows")
```

## Troubleshooting

### Issue: `[CANNOT_ACCEPT_OBJECT_IN_TYPE] DateType() can not accept object ... in type str`
**Cause:** Date columns require proper date objects, not strings  
**Solution:** Already fixed in notebook - uses `dt_date(2025, 1, 1)` instead of `"2025-01-01"`  
**Note:** PySpark requires strict type matching for schema compliance

### Issue: `gold.baskets_enriched` table not found
**Cause:** Step 3 not completed  
**Solution:** Run `09_metadata_joins.py` first to create the gold table

### Issue: Only 1 version in history
**Cause:** Table just created, hasn't been modified yet  
**Solution:** This is expected for brand new tables. The demo inserts+deletes to create history.

### Issue: Time travel error "Version X is not available"
**Cause:** Trying to access a version that doesn't exist or was VACUUMed  
**Solution:** Check `DESCRIBE HISTORY` for available versions

### Issue: DELETE doesn't work
**Cause:** Table is not Delta format  
**Solution:** Ensure table was created with `.saveAsTable()` (not CSV/Parquet)

## Production Best Practices

### 1. Retention Period
Delta Lake keeps version history for 30 days by default:
```sql
-- Change retention to 90 days
ALTER TABLE gold.baskets_enriched 
SET TBLPROPERTIES (delta.logRetentionDuration = "interval 90 days");
```

### 2. VACUUM Old Files
Clean up old versions to save storage:
```sql
-- Remove files older than retention period
VACUUM gold.baskets_enriched;

-- Dry run to see what would be deleted
VACUUM gold.baskets_enriched RETAIN 168 HOURS DRY RUN;
```

### 3. Monitoring
Track version history growth:
```python
history = delta_table.history()
print(f"Total versions: {history.count()}")
print(f"Oldest version: {history.orderBy('version').first()['timestamp']}")
```

## Screenshots Required

📸 **Capture the DESCRIBE HISTORY output** and save as:
`docs/screenshots/delta_history.png`

This shows:
- Version progression (0 → 1 → 2 → ...)
- Operations (WRITE, DELETE)
- Timestamps
- Metrics (rows affected)

## Next Steps

Once Step 4 completes successfully:
1. Mark Step 4 complete ✓
2. Proceed to **Step 5**: `11_perf_skew_broadcast.py` (performance tuning)
3. See `docs/IMPLEMENTATION_PLAN.md` for remaining steps

## Exit Criteria Checklist

- [ ] Notebook runs without errors
- [ ] Bad row successfully inserted and verified
- [ ] DELETE operation removes bad row
- [ ] DESCRIBE HISTORY shows at least 3 versions
- [ ] Time travel reads old version with bad row
- [ ] Current version is clean (no bad row)
- [ ] ADD COLUMN creates new column with default value
- [ ] RENAME COLUMN changes column name successfully
- [ ] DROP COLUMN removes column from schema
- [ ] Time travel still works after schema changes
- [ ] Screenshot of history saved
- [ ] Rollback patterns documented

**When all boxes checked, Step 4 is complete!** 🎉

## Business Value Summary

**Compliance:** Full audit trail for regulations (GDPR, SOX, HIPAA)  
**Recovery:** Rollback from mistakes in minutes, not hours  
**Debugging:** See exactly what changed and when  
**Confidence:** Can't permanently lose data (within retention period)  
**Schema Flexibility:** Add/rename/drop columns without downtime  
**Backward Compatibility:** Old queries work with time travel  
**Cost:** No extra setup needed - built into Delta Lake

