# IKEA Lakehouse: Unit Tests

This directory contains unit tests for data transformation logic in the IKEA retail analytics lakehouse.

## Quick Start

```bash
# Install dependencies
pip install -r ../requirements.txt

# Run all tests
pytest test_transforms.py -v

# Run with coverage
pytest test_transforms.py -v --cov=. --cov-report=term-missing
```

## Test Structure

```
tests/
├── __init__.py                      # Package marker
├── test_transforms.py               # Main test suite (13 tests)
├── expectations/                    # Great Expectations suites (optional)
│   ├── fx_rates_suite.json         # FX rates quality expectations
│   └── README.md                    # GE usage guide
└── README.md                        # This file
```

## Test Classes

### `TestFXTransform` (5 tests)
Tests for `silver.fx_rates_daily` transformation logic:
- ✅ Schema compliance
- ✅ Null primary key checks
- ✅ Deduplication correctness
- ✅ Rate bounds validation
- ✅ Composite key uniqueness

### `TestProductTransform` (3 tests)
Tests for `silver.dim_products_api` transformation logic:
- ✅ Size class mapping (LARGE vs SMALL)
- ✅ Product deduplication
- ✅ Null product_id filtering

### `TestCustomerTransform` (2 tests)
Tests for `silver.dim_customers_api` transformation logic:
- ✅ Full name concatenation
- ✅ Null customer_id filtering

### `TestDataQualityGates` (3 tests)
Generic data quality checks:
- ✅ Duplicate key detection
- ✅ Orphan foreign key detection
- ✅ Date range validation

### `TestEndToEndTransform` (integration)
Integration tests requiring Databricks environment (skipped in local runs).

## Running Specific Tests

```bash
# Run only FX transform tests
pytest test_transforms.py::TestFXTransform -v

# Run a single test
pytest test_transforms.py::TestFXTransform::test_deduplication_logic -v

# Skip integration tests
pytest test_transforms.py -v -m "not integration"
```

## Test Dependencies

- **pytest**: Test framework
- **chispa**: PySpark DataFrame assertions
- **pyspark**: Local Spark session for testing

All dependencies are listed in `../requirements.txt`.

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Data Quality Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.10'
      - run: pip install -r requirements.txt
      - run: pytest tests/test_transforms.py -v --tb=short
```

### Databricks Job Example

```python
# Notebook: run_tests.py
%pip install pytest chispa

# Run tests
import subprocess
result = subprocess.run(
    ["pytest", "/Workspace/Repos/<repo>/tests/test_transforms.py", "-v"],
    capture_output=True,
    text=True
)

print(result.stdout)

if result.returncode != 0:
    dbutils.notebook.exit(json.dumps({"status": "FAILED"}))
else:
    dbutils.notebook.exit(json.dumps({"status": "SUCCESS"}))
```

## What This Tests

These tests validate the **transformation logic** from:
- `notebooks/07_ingest_api_data.py` (bronze ingestion)
- `notebooks/08_silver_api_transform.sql` (silver CTEs)

By testing transformation logic in isolation, we can:
1. **Catch bugs early** before they reach production
2. **Validate schema changes** don't break downstream consumers
3. **Ensure data quality** (bounds, nulls, duplicates)
4. **Document behavior** through test cases

## Further Reading

- **Quick Start Guide**: `../docs/STEP6_QUICK_START.md`
- **Summary**: `../docs/STEP6_SUMMARY.md`
- **Great Expectations**: `expectations/README.md`
- **pytest Docs**: https://docs.pytest.org/
- **chispa Docs**: https://github.com/MrPowers/chispa

