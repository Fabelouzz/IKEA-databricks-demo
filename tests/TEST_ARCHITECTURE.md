# Test Architecture

This document explains the testing strategy and architecture for the IKEA Lakehouse project.

## Testing Pyramid

```
                    ┌─────────────────┐
                    │   Integration   │  ← Databricks environment
                    │   Tests (few)   │     End-to-end validation
                    └─────────────────┘
                           ▲
                           │
                  ┌────────────────────┐
                  │  Component Tests   │  ← Test transform modules
                  │    (moderate)      │     FX, Product, Customer
                  └────────────────────┘
                           ▲
                           │
              ┌──────────────────────────┐
              │     Unit Tests (many)    │  ← Test individual functions
              │  Schema, Dedup, Quality  │     Fast, no dependencies
              └──────────────────────────┘
```

## Test Layers

### Layer 1: Unit Tests (Most Tests)
**Location**: `tests/test_transforms.py`  
**Purpose**: Test individual transformation logic in isolation  
**Speed**: Fast (~8 seconds for all 13 tests)  
**Dependencies**: None (local PySpark session)

**Examples**:
- Schema validation
- Deduplication logic with ROW_NUMBER()
- Data quality bounds (rate > 0)
- Null filtering

### Layer 2: Component Tests (Moderate)
**Location**: `tests/test_transforms.py` (test classes)  
**Purpose**: Test entire transformation modules (FX, Product, Customer)  
**Speed**: Moderate  
**Dependencies**: Local PySpark

**Examples**:
- `TestFXTransform`: All FX rate transformations
- `TestProductTransform`: All product dimension logic
- `TestCustomerTransform`: All customer dimension logic

### Layer 3: Integration Tests (Fewest)
**Location**: `tests/test_transforms.py` (marked with `@pytest.mark.integration`)  
**Purpose**: Test end-to-end pipelines in Databricks  
**Speed**: Slow  
**Dependencies**: Databricks cluster, actual bronze/silver tables

**Examples**:
- Bronze → Silver full pipeline
- Silver → Gold join pipeline
- Data quality validation on real data

## Test Execution Flow

```
┌─────────────────┐
│  Developer      │
│  Writes Code    │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│  Run Unit Tests Locally │  ← pytest tests/test_transforms.py -v
│  (venv, local PySpark)  │     Fast feedback (8 seconds)
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Commit & Push to Git   │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  CI/CD Pipeline (GH)    │  ← GitHub Actions runs unit tests
│  Run Unit Tests         │     Blocks merge if tests fail
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Deploy to Databricks   │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Databricks Job         │  ← Post-deployment validation
│  Run Integration Tests  │     Validates on real cluster
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Monitor Test Results   │  ← Log results to Delta table
│  Alert on Failures      │     Track pass/fail rates over time
└─────────────────────────┘
```

## Test Data Strategy

### Synthetic Test Data (Unit Tests)
- **Creation**: `spark.createDataFrame([...], schema)`
- **Advantages**:
  - Fast (no I/O)
  - Deterministic (same results every run)
  - Edge cases easy to test (nulls, duplicates, boundaries)
- **Disadvantages**:
  - May not reflect real data distribution

**Example**:
```python
bronze_data = [
    ("2025-01-01", "EUR", "SEK", 11.5, "2025-01-01T10:00:00"),
    ("2025-01-01", "EUR", "SEK", 11.6, "2025-01-01T10:00:00"),  # Duplicate
]
df_bronze = spark.createDataFrame(bronze_data, schema)
```

### Real Data Samples (Integration Tests)
- **Creation**: `spark.table("bronze.fx_rates_raw").limit(1000)`
- **Advantages**:
  - Tests real data scenarios
  - Catches unexpected data quality issues
- **Disadvantages**:
  - Slower (network I/O)
  - Non-deterministic (data changes over time)

**Example**:
```python
df_bronze = spark.table("bronze.fx_rates_raw").filter("as_of_date >= '2025-01-01'")
df_silver = transform_fx_rates(df_bronze)
assert df_silver.count() > 0
```

## Test Assertions with Chispa

### Why Chispa?

Standard PySpark DataFrame comparison is painful:
```python
# ❌ Hard to debug
assert df1.collect() == df2.collect()
# Error: [Row(...), Row(...)] != [Row(...), Row(...)]
```

Chispa provides clear, readable assertions:
```python
# ✅ Clear error messages
assert_df_equality(df1, df2, ignore_row_order=True)
# Error:
# Expected:
#   +------+------+
#   | pair | rate |
#   +------+------+
#   | EUR/SEK | 11.6 |
#   +------+------+
# Actual:
#   +------+------+
#   | pair | rate |
#   +------+------+
#   | EUR/SEK | 11.5 |  ← MISMATCH
#   +------+------+
```

### Chispa Assertion Types

```python
# 1. Exact equality (order matters)
assert_df_equality(df_actual, df_expected)

# 2. Ignore row order
assert_df_equality(df_actual, df_expected, ignore_row_order=True)

# 3. Ignore column order
assert_df_equality(df_actual, df_expected, ignore_column_order=True)

# 4. Approximate equality (for floats)
assert_approx_df_equality(df_actual, df_expected, precision=0.01)

# 5. Column equality (single column)
assert_column_equality(df, "column1", "column2")
```

## Mocking & Fixtures

### Spark Session Fixture
```python
@pytest.fixture(scope="session")
def spark():
    """
    Shared Spark session for all tests.
    
    scope="session": Created once, reused by all tests (faster)
    scope="function": Created per test (isolated but slower)
    """
    return SparkSession.builder.master("local[2]").getOrCreate()
```

### Sample Data Fixtures (Future)
```python
@pytest.fixture
def sample_fx_data(spark):
    """Reusable sample FX data for multiple tests."""
    return spark.createDataFrame([
        ("2025-01-01", "EUR", "SEK", 11.5),
        ("2025-01-02", "EUR", "USD", 1.08),
    ], ["as_of_date", "base_currency", "quote_currency", "rate"])

def test_something(spark, sample_fx_data):
    # Use sample_fx_data directly
    assert sample_fx_data.count() == 2
```

## Test Markers (pytest)

### Marking Integration Tests
```python
@pytest.mark.integration
def test_bronze_to_silver_pipeline(spark):
    """Requires Databricks environment."""
    pytest.skip("Integration test")
```

**Run only unit tests**:
```bash
pytest tests/ -v -m "not integration"
```

**Run only integration tests**:
```bash
pytest tests/ -v -m "integration"
```

### Marking Slow Tests
```python
@pytest.mark.slow
def test_large_dataset_transform(spark):
    """Takes 60+ seconds."""
    df = spark.range(0, 10_000_000)
    # ... expensive operation
```

**Skip slow tests during development**:
```bash
pytest tests/ -v -m "not slow"
```

## Coverage Reporting

### Generate Coverage Report
```bash
pytest tests/test_transforms.py -v --cov=tests --cov-report=html
```

**Output**:
```
---------- coverage: platform darwin, python 3.10.11 -----------
Name                      Stmts   Miss  Cover
---------------------------------------------
tests/test_transforms.py    150      0   100%
---------------------------------------------
TOTAL                       150      0   100%

Coverage HTML written to dir htmlcov
```

### View HTML Report
```bash
open htmlcov/index.html
```

Shows line-by-line coverage with green (covered) and red (not covered) highlighting.

## Best Practices

### ✅ DO

1. **Test transformation logic, not Spark internals**
   ```python
   # ✅ Good: Test our business logic
   def test_size_class_mapping(spark):
       df = spark.createDataFrame([(1500.0,)], ["price"])
       result = df.withColumn("size_class", F.when(F.col("price") > 1000, "LARGE").otherwise("SMALL"))
       assert result.select("size_class").collect()[0][0] == "LARGE"
   ```

2. **Use descriptive test names**
   ```python
   # ✅ Good: Clear what's being tested
   def test_deduplication_keeps_latest_ingestion_by_composite_key(spark):
       ...
   ```

3. **Arrange-Act-Assert pattern**
   ```python
   def test_something(spark):
       # Arrange: Set up test data
       df_input = spark.createDataFrame([...])
       
       # Act: Perform transformation
       df_result = transform(df_input)
       
       # Assert: Verify outcome
       assert df_result.count() == expected_count
   ```

4. **Test edge cases**
   ```python
   # Nulls, duplicates, boundaries, empty datasets
   def test_handles_null_rate(spark):
       df = spark.createDataFrame([(None,)], ["rate"])
       df_clean = df.filter("rate IS NOT NULL")
       assert df_clean.count() == 0
   ```

### ❌ DON'T

1. **Don't test Spark's built-in functions**
   ```python
   # ❌ Bad: Testing Spark, not our logic
   def test_concat_works(spark):
       df = spark.createDataFrame([("a", "b")], ["c1", "c2"])
       result = df.withColumn("c3", F.concat("c1", "c2"))
       assert result.select("c3").collect()[0][0] == "ab"
   ```

2. **Don't make tests dependent on external state**
   ```python
   # ❌ Bad: Test fails if table doesn't exist
   def test_something(spark):
       df = spark.table("production.sales")  # External dependency
       assert df.count() > 0
   ```

3. **Don't use magic numbers without context**
   ```python
   # ❌ Bad: What does 11.6 represent?
   assert df.select("rate").collect()[0][0] == 11.6
   
   # ✅ Good: Clear meaning
   EXPECTED_LATEST_RATE = 11.6  # Latest ingestion wins in dedup
   assert df.select("rate").collect()[0][0] == EXPECTED_LATEST_RATE
   ```

## Future Enhancements

### 1. Property-Based Testing (Hypothesis)
```python
from hypothesis import given, strategies as st

@given(st.floats(min_value=0.01, max_value=1000))
def test_all_valid_rates_pass_quality_check(rate):
    """Hypothesis generates 100s of random test cases."""
    df = spark.createDataFrame([(rate,)], ["rate"])
    df_valid = df.filter("rate > 0")
    assert df_valid.count() == 1
```

### 2. Snapshot Testing
```python
# Save expected output once
df_expected = transform(df_input)
df_expected.write.mode("overwrite").saveAsTable("tests.snapshots.fx_transform_v1")

# On subsequent runs, compare to snapshot
df_actual = transform(df_input)
df_expected = spark.table("tests.snapshots.fx_transform_v1")
assert_df_equality(df_actual, df_expected)
```

### 3. Performance Regression Tests
```python
import time

def test_transform_performance(spark):
    """Ensure transform completes in < 10 seconds."""
    df = spark.range(0, 1_000_000)
    
    start = time.time()
    result = expensive_transform(df)
    result.count()  # Force execution
    duration = time.time() - start
    
    assert duration < 10.0, f"Transform too slow: {duration:.2f}s"
```

## Summary

**Testing Philosophy**: Data transformations are code. Test them like code.

**Test Coverage**:
- Unit tests: 13 tests, 100% coverage of transformation logic
- Component tests: 4 test classes (FX, Product, Customer, Quality)
- Integration tests: Stubbed for Databricks execution

**Tools**:
- pytest: Test framework
- chispa: PySpark-specific assertions
- Great Expectations: Optional enterprise data quality

**Execution Time**: ~8 seconds for all unit tests locally

**CI/CD Ready**: Tests can run in GitHub Actions, Databricks Jobs, or local development

