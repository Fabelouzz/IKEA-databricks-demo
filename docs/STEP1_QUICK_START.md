# Step 1: API Ingestion - Quick Start Guide

## What Was Implemented

### Files Created
1. **`notebooks/07_ingest_api_data.py`** - Main ingestion notebook with built-in tests
2. **`ops/postman_collection.json`** - API validation collection for Postman
3. **`docs/screenshots/`** - Directory for validation screenshots

## How to Run on Databricks

### Prerequisites
- Databricks workspace with a running cluster (DBR 14.x+, Python 3.10)
- Cursor connected to Databricks via the extension
- Network access to public APIs (DummyJSON and Frankfurter)

### Step-by-Step Execution

#### 1. Open the Notebook
- In Databricks, navigate to `notebooks/07_ingest_api_data.py`
- Attach to a running cluster

#### 2. Run All Cells
Execute cells in order (or "Run All"):

**Cells 1-3:** Install dependencies, import libraries  
**Cells 4-6:** Fetch DummyJSON products with pagination → `bronze.products_raw`  
**Cells 7-8:** Fetch DummyJSON users → `bronze.users_raw`  
**Cells 9-11:** Fetch Frankfurter FX rates → `bronze.fx_rates_raw`  
**Cells 12-20:** Built-in validation tests (see below)

#### 3. Review Test Results
The notebook automatically runs 6 validation tests:

| Test | What It Checks | Expected Result |
|------|----------------|-----------------|
| **A. Tables exist** | `SHOW TABLES IN bronze` | 3 tables: products_raw, users_raw, fx_rates_raw |
| **B. Pagination** | Compare API total vs bronze count | Counts match (~194 products, ~200 users) |
| **C. Schema** | `DESCRIBE bronze.products_raw` | Correct types: id (INT), ingested_at (STRING), etc. |
| **D. Metadata** | Check `ingested_at`, `source` columns | All rows populated |
| **E. Quality** | FX rates > 0 | Zero non-positive rates |
| **F. Idempotency** | `COUNT(*) = COUNT(DISTINCT id)` | No duplicate IDs |

**All tests should show ✓ PASS**

#### 4. Verify Bronze Tables

```sql
-- Check row counts
SELECT 'products' AS t, COUNT(*) FROM bronze.products_raw
UNION ALL SELECT 'users', COUNT(*) FROM bronze.users_raw
UNION ALL SELECT 'fx_rates', COUNT(*) FROM bronze.fx_rates_raw;
```

Expected output:
```
products    | ~194
users       | ~200
fx_rates    | ~180 (90 days × 2 currencies)
```

#### 5. Test Idempotency
Re-run the entire notebook. Counts should remain stable (overwrite mode prevents duplicates).

## Postman Validation (Manual)

### Import Collection
1. Open Postman
2. File → Import → `ops/postman_collection.json`
3. Collection "IKEA Demo - API Validation" appears

### Test Requests
Execute these requests to validate API contracts:

1. **DummyJSON - GET Products (Pagination)**
   - Demonstrates pagination with `limit` and `skip` params
   - Expected: 200 OK, JSON with 10 products

2. **DummyJSON - POST Products Search**
   - Demonstrates POST with JSON body (`{"q": "phone"}`)
   - Expected: 200 OK, filtered products

3. **Frankfurter - GET FX Rates (Date Range)**
   - Demonstrates time-series with date range in path
   - Expected: 200 OK, rates for EUR/SEK, EUR/USD

### Screenshot for Documentation
- Execute "DummyJSON - GET Products" in Postman
- Capture the request + response (showing 200 OK)
- Save as `docs/screenshots/api_validation.png`

## What This Proves (Interview Skills)

| Skill | Evidence in Notebook |
|-------|---------------------|
| **GET vs POST** | Postman collection shows both methods with rationale |
| **Pagination** | `while True` loop with `skip` offset increments |
| **Retry logic** | `@retry(stop_max_attempt_number=3)` decorator |
| **Schema-on-write** | Explicit `StructType` definitions (no `inferSchema`) |
| **Idempotency** | `mode("overwrite")` prevents duplicates on re-run |
| **Lineage** | `ingested_at`, `source`, `source_url` metadata columns |
| **Data quality** | Positive rate checks, null filters |
| **Testing** | 6 automated validation tests in notebook |

## Troubleshooting

### Issue: `[CANNOT_ACCEPT_OBJECT_IN_TYPE] DoubleType() can not accept object X in type int`
**Cause:** PySpark strict type checking - API returns integers but schema expects floats  
**Solution:** Already fixed in notebook - numeric fields are explicitly cast to `float()`. If you see this, make sure you're using the latest version of the notebook.

### Issue: `requests` module not found
**Solution:** Ensure first cell (`%pip install requests retrying`) runs successfully. Wait for kernel restart if needed.

### Issue: API timeout or network error
**Solution:** Check cluster has internet access. Retry decorator will attempt 3 times automatically.

### Issue: Tables already exist error
**Solution:** Using `mode("overwrite")` should handle this. If persists, manually drop: `DROP TABLE IF EXISTS bronze.products_raw;`

### Issue: Pagination count mismatch
**Solution:** DummyJSON API may have added/removed products. As long as counts are close (~190-200), pagination is working.

## Next Steps

Once Step 1 tests pass:
1. Mark Step 1 complete ✓
2. Proceed to **Step 2**: `08_silver_api_transform.sql` (CTE-based transforms)
3. See `docs/IMPLEMENTATION_PLAN.md` for full roadmap

## Exit Criteria Checklist

- [ ] Notebook runs without errors
- [ ] 3 bronze tables created with non-zero rows
- [ ] Pagination test passes (API total = bronze count)
- [ ] Schema has required columns (id, ingested_at, source)
- [ ] Metadata columns populated
- [ ] FX rates all positive
- [ ] No duplicate product/user IDs
- [ ] Postman collection imported and tested
- [ ] Screenshot saved to `docs/screenshots/api_validation.png`

**When all boxes checked, Step 1 is complete!** 🎉

