# Step 2: SQL Transforms with CTEs - Quick Start Guide

## What Was Implemented

### Files Created
1. **`notebooks/08_silver_api_transform.sql`** - SQL notebook with CTE-based transforms

### Silver Tables Created
1. **`silver.fx_rates_daily`** - Daily FX rates (composite PK: pair, as_of_date)
2. **`silver.dim_currency`** - Currency dimension (PK: currency_code)
3. **`silver.dim_products_api`** - Product dimension from DummyJSON (PK: product_id)
4. **`silver.dim_customers_api`** - Customer dimension from DummyJSON (PK: customer_id)

## Prerequisites

**Step 1 must be complete:**
- ✅ `bronze.products_raw` exists (~194 rows)
- ✅ `bronze.users_raw` exists (~208 rows)
- ✅ `bronze.fx_rates_raw` exists (~128 rows)

## How to Run on Databricks

### Step-by-Step Execution

#### 1. Open the SQL Notebook
- In Databricks, navigate to `notebooks/08_silver_api_transform.sql`
- Attach to a running cluster (same cluster from Step 1 works)

#### 2. Run All Cells
Execute cells in order (or "Run All"):

**Section 1: FX Rates Daily (cells 1-6)**
- CTE-based transform: clean → dedup → latest
- Creates `silver.fx_rates_daily`
- Tests: duplicate check (should be 0)

**Section 2: Dim Currency (cells 7-10)**
- Distinct currency codes from FX data
- Creates `silver.dim_currency`
- Tests: referential integrity check

**Section 3: Dim Products (cells 11-16)**
- Products from DummyJSON with size_class mapping
- Creates `silver.dim_products_api`
- Tests: PK uniqueness, size_class logic

**Section 4: Dim Customers (cells 17-20)**
- Customers from DummyJSON users
- Creates `silver.dim_customers_api`
- Tests: PK uniqueness, null checks

**Section 5: Summary (cells 21-28)**
- Row counts for all silver tables
- Sample data verification

#### 3. Review Test Results

The notebook includes **8 automated validation queries**:

| Test | What It Checks | Expected Result |
|------|----------------|-----------------|
| **1. FX duplicates** | Composite key (pair, as_of_date) uniqueness | 0 duplicates |
| **2. FK integrity** | All FX currencies exist in dim_currency | 0 orphan records |
| **3. Product PKs** | product_id uniqueness | 0 duplicates |
| **4. Size class** | price > 1000 → LARGE, else SMALL | LARGE min > 1000, SMALL max ≤ 1000 |
| **5. Customer PKs** | customer_id uniqueness | 0 duplicates |
| **6. Customer nulls** | No null IDs/names/emails | All counts equal |
| **7. Row counts** | All 4 silver tables created | 4 rows returned |
| **8. Sample data** | Visual inspection of data quality | Spot check values |

**All duplicate checks should return 0 rows**

#### 4. Verify Silver Tables

```sql
-- Check all silver tables exist
SHOW TABLES IN silver;
```

Expected output:
```
dim_currency
dim_customers_api
dim_products_api
fx_rates_daily
```

```sql
-- Check row counts
SELECT 'fx_rates_daily' AS t, COUNT(*) FROM silver.fx_rates_daily
UNION ALL SELECT 'dim_currency', COUNT(*) FROM silver.dim_currency
UNION ALL SELECT 'dim_products_api', COUNT(*) FROM silver.dim_products_api
UNION ALL SELECT 'dim_customers_api', COUNT(*) FROM silver.dim_customers_api;
```

Expected output:
```
fx_rates_daily     | ~128
dim_currency       | 2-3
dim_products_api   | 194
dim_customers_api  | 208
```

## What This Proves (Interview Skills)

| Skill | Evidence in Notebook |
|-------|---------------------|
| **CTE structure** | 3-step CTE: clean_rates → deduped → latest_by_day |
| **Composite keys** | (pair, as_of_date) documented and tested |
| **Primary keys** | product_id, customer_id, currency_code |
| **Referential integrity** | FK check: fx_rates → dim_currency |
| **Deduplication** | ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...) |
| **Data quality** | Null filters, positive rate checks, uniqueness tests |
| **Business logic** | size_class mapping (price threshold) |
| **SQL readability** | Clear comments, multi-step CTEs vs nested subqueries |

## Key SQL Patterns Demonstrated

### 1. CTE-Based Transform (Clean → Dedup → Select)
```sql
WITH clean_rates AS (
  SELECT ... WHERE rate > 0 AND ...  -- Data quality
),
deduped AS (
  SELECT ..., ROW_NUMBER() OVER (...) AS rn  -- Deduplication
),
latest_by_day AS (
  SELECT ... WHERE rn = 1  -- Filter to latest
)
SELECT ... FROM latest_by_day;
```

### 2. Composite Key Documentation
```sql
-- PRIMARY KEY (pair, as_of_date)  -- Conceptual PK documented
```

### 3. Deduplication with Window Functions
```sql
ROW_NUMBER() OVER (
  PARTITION BY pair, as_of_date 
  ORDER BY ingested_at DESC
) AS rn
```

### 4. Referential Integrity Check
```sql
WHERE SPLIT(pair, '/')[1] NOT IN (SELECT currency_code FROM silver.dim_currency)
-- Should return 0 rows
```

## Troubleshooting

### Issue: Table already exists error
**Solution:** Using `CREATE OR REPLACE TABLE` should handle this. Notebook is idempotent.

### Issue: Bronze tables not found
**Solution:** Run Step 1 first (`07_ingest_api_data.py`) to create bronze tables.

### Issue: Duplicate composite key errors
**Solution:** The dedup CTE should prevent this. If duplicates appear, check that ROW_NUMBER() logic is correct and `WHERE rn = 1` is applied.

### Issue: Size_class shows LARGE items with price ≤ 1000
**Solution:** Verify the CASE statement threshold. Current logic: `WHEN price > 1000 THEN 'LARGE'`

## Next Steps

Once Step 2 tests pass:
1. Mark Step 2 complete ✓
2. Proceed to **Step 3**: `09_metadata_joins.py` (config-driven joins to gold)
3. See `docs/IMPLEMENTATION_PLAN.md` for full roadmap

## Exit Criteria Checklist

- [ ] Notebook runs without errors
- [ ] 4 silver tables created
- [ ] All duplicate checks return 0 rows
- [ ] Row counts match expected values
- [ ] Referential integrity check passes (0 orphan FX currencies)
- [ ] Size_class logic correct (LARGE min > 1000, SMALL max ≤ 1000)
- [ ] Sample data looks correct (visual spot check)

**When all boxes checked, Step 2 is complete!** 🎉

