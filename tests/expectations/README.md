# Great Expectations Suites

This directory contains optional Great Expectations suite definitions for data quality validation.

## What is Great Expectations?

Great Expectations is a Python framework for data quality testing and documentation. It allows you to define expectations (assertions) about your data and validate them automatically.

## Available Suites

- **`fx_rates_suite.json`**: Quality expectations for `silver.fx_rates_daily` table
  - Validates composite PK uniqueness
  - Checks rate bounds (0.01 to 1000)
  - Validates pair format (e.g., EUR/SEK)
  - Ensures no null values in key columns

## How to Use (Optional)

### 1. Install Great Expectations

```bash
pip install great-expectations>=0.18.0
```

### 2. Initialize Great Expectations Context

```python
from great_expectations.data_context import FileDataContext
import great_expectations as gx

# Create context (first time only)
context = gx.get_context()

# Or load existing
context = FileDataContext.create(".")
```

### 3. Run Expectations in Databricks

```python
# In a Databricks notebook
from great_expectations.dataset import SparkDFDataset
import json

# Load suite
with open("tests/expectations/fx_rates_suite.json") as f:
    suite = json.load(f)

# Load table
df = spark.table("silver.fx_rates_daily")

# Validate
ge_df = SparkDFDataset(df)
results = ge_df.validate(expectation_suite=suite)

# Check results
if results["success"]:
    print("✓ All expectations passed!")
else:
    print("✗ Some expectations failed:")
    for result in results["results"]:
        if not result["success"]:
            print(f"  - {result['expectation_config']['expectation_type']}")
```

### 4. Integration with CI/CD

Add expectations validation to your Databricks Job:

```python
# Task: Validate silver.fx_rates_daily quality
# Depends on: 08_silver_api_transform.sql

results = validate_table("silver.fx_rates_daily", "fx_rates_suite.json")

if not results["success"]:
    dbutils.notebook.exit(json.dumps({"status": "FAILED", "reason": "Data quality checks failed"}))
```

## Why Use Great Expectations?

1. **Documentation**: Expectations serve as living documentation of your data contracts
2. **Automation**: Catch data quality issues before they reach downstream consumers
3. **Observability**: Track data quality metrics over time
4. **Standardization**: Consistent validation logic across teams

## Alternative: Pytest-Based Validation

If you prefer lightweight testing without GE, use the pytest tests in `tests/test_transforms.py` which cover the same validation logic.

