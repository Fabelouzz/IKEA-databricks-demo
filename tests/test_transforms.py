"""
Unit tests for data transformations.
Uses pytest + chispa for PySpark DataFrame assertions.

Run:
  pytest tests/test_transforms.py -v
  pytest tests/test_transforms.py -v --cov=tests --cov-report=term-missing
"""

import pytest
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *
from chispa.dataframe_comparer import assert_df_equality
from datetime import date


@pytest.fixture(scope="session")
def spark():
    """Create Spark session for testing."""
    return (SparkSession.builder
            .master("local[2]")
            .appName("test_transforms")
            .config("spark.sql.shuffle.partitions", "2")  # Speed up tests
            .getOrCreate())


class TestFXTransform:
    """Test silver.fx_rates_daily CTE logic."""
    
    def test_schema_compliance(self, spark):
        """Verify output schema matches expected."""
        # Simulate bronze input
        bronze_data = [
            ("2025-01-01", "EUR", "SEK", 11.5, "2025-01-01T10:00:00", "api.test"),
            ("2025-01-02", "EUR", "USD", 1.08, "2025-01-02T10:00:00", "api.test"),
        ]
        df_bronze = spark.createDataFrame(bronze_data, 
            ["as_of_date", "base_currency", "quote_currency", "rate", "ingested_at", "source_url"])
        
        # Apply transformation (same logic as 08_silver_api_transform.sql)
        df_silver = (df_bronze
                     .withColumn("pair", F.concat_ws("/", "base_currency", "quote_currency"))
                     .select("as_of_date", "pair", "rate"))
        
        # Assert schema
        expected_schema = StructType([
            StructField("as_of_date", StringType(), True),
            StructField("pair", StringType(), True),
            StructField("rate", DoubleType(), True),
        ])
        
        assert df_silver.schema == expected_schema, "Schema mismatch"
        print("✓ Schema validation passed")
    
    def test_no_null_primary_keys(self, spark):
        """Ensure PK columns (pair, as_of_date) are never null."""
        bronze_data = [
            ("2025-01-01", "EUR", "SEK", 11.5),
            (None, "EUR", "USD", 1.08),  # Bad row - null date
            ("2025-01-03", None, "SEK", 11.6),  # Bad row - null base
        ]
        df_bronze = spark.createDataFrame(bronze_data, 
            ["as_of_date", "base_currency", "quote_currency", "rate"])
        
        # Clean (filter nulls) - same as CTE in SQL notebook
        df_clean = df_bronze.filter(
            "as_of_date IS NOT NULL AND base_currency IS NOT NULL AND quote_currency IS NOT NULL"
        )
        
        # Assert no nulls in PK columns
        null_count = df_clean.filter(
            "as_of_date IS NULL OR base_currency IS NULL OR quote_currency IS NULL"
        ).count()
        
        assert null_count == 0, f"Found {null_count} rows with null PK columns"
        assert df_clean.count() == 1, "Should have 1 valid row after filtering"
        print("✓ Primary key null check passed")
    
    def test_deduplication_logic(self, spark):
        """Verify dedup keeps latest ingestion for duplicate (pair, date)."""
        # Duplicate data: same pair+date, different rates (simulate re-ingestion)
        bronze_data = [
            ("2025-01-01", "EUR", "SEK", 11.5, "2025-01-01T08:00:00"),
            ("2025-01-01", "EUR", "SEK", 11.6, "2025-01-01T10:00:00"),  # Later ingestion - should win
            ("2025-01-02", "EUR", "USD", 1.08, "2025-01-02T10:00:00"),
        ]
        df_bronze = spark.createDataFrame(bronze_data, 
            ["as_of_date", "base_currency", "quote_currency", "rate", "ingested_at"])
        
        # Dedup logic (same as CTE 2 in 08_silver_api_transform.sql)
        df_deduped = (df_bronze
                      .withColumn("pair", F.concat_ws("/", "base_currency", "quote_currency"))
                      .withColumn("rn", F.row_number().over(
                          Window.partitionBy("pair", "as_of_date").orderBy(F.desc("ingested_at"))
                      ))
                      .filter("rn = 1")
                      .select("as_of_date", "pair", "rate"))
        
        # Expected: only latest rate for 2025-01-01 EUR/SEK (11.6, not 11.5)
        expected_data = [
            ("2025-01-01", "EUR/SEK", 11.6),
            ("2025-01-02", "EUR/USD", 1.08),
        ]
        df_expected = spark.createDataFrame(expected_data, ["as_of_date", "pair", "rate"])
        
        assert_df_equality(df_deduped, df_expected, ignore_row_order=True)
        print("✓ Deduplication logic passed")
    
    def test_rate_bounds(self, spark):
        """Ensure rates are positive (data quality check)."""
        bronze_data = [
            ("2025-01-01", "EUR", "SEK", 11.5),
            ("2025-01-02", "EUR", "USD", -1.0),  # Invalid - negative
            ("2025-01-03", "EUR", "GBP", 0.0),   # Invalid - zero
        ]
        df_bronze = spark.createDataFrame(bronze_data, 
            ["as_of_date", "base_currency", "quote_currency", "rate"])
        
        # Quality check (same as CTE 1 in SQL notebook)
        df_valid = df_bronze.filter("rate > 0")
        
        assert df_valid.count() == 1, "Should filter out non-positive rates"
        assert df_valid.select("rate").collect()[0][0] == 11.5, "Should keep only valid rate"
        print("✓ Rate bounds check passed")
    
    def test_composite_key_uniqueness(self, spark):
        """Verify (pair, as_of_date) composite key is unique after transform."""
        bronze_data = [
            ("2025-01-01", "EUR", "SEK", 11.5, "2025-01-01T10:00:00"),
            ("2025-01-01", "EUR", "USD", 1.08, "2025-01-01T10:00:00"),
            ("2025-01-02", "EUR", "SEK", 11.6, "2025-01-02T10:00:00"),
        ]
        df_bronze = spark.createDataFrame(bronze_data, 
            ["as_of_date", "base_currency", "quote_currency", "rate", "ingested_at"])
        
        # Transform
        df_silver = (df_bronze
                     .withColumn("pair", F.concat_ws("/", "base_currency", "quote_currency"))
                     .select("as_of_date", "pair", "rate"))
        
        # Check for duplicates on composite key
        duplicates = (df_silver
                      .groupBy("pair", "as_of_date")
                      .count()
                      .filter("count > 1"))
        
        assert duplicates.count() == 0, "Composite key (pair, as_of_date) should be unique"
        print("✓ Composite key uniqueness passed")


class TestProductTransform:
    """Test silver.dim_products_api logic."""
    
    def test_size_class_mapping(self, spark):
        """Verify price > 1000 maps to LARGE, else SMALL."""
        products = [
            (1, "Sofa", 1500.0),     # Should be LARGE
            (2, "Lamp", 50.0),       # Should be SMALL
            (3, "Table", 999.0),     # Should be SMALL (boundary)
            (4, "Bed", 1000.01),     # Should be LARGE (boundary)
        ]
        df = spark.createDataFrame(products, ["product_id", "title", "price"])
        
        # Apply transformation (same logic as 08_silver_api_transform.sql)
        df_transformed = df.withColumn("size_class", 
                                       F.when(F.col("price") > 1000, "LARGE").otherwise("SMALL"))
        
        # Assert each case
        assert df_transformed.filter("product_id = 1").select("size_class").collect()[0][0] == "LARGE"
        assert df_transformed.filter("product_id = 2").select("size_class").collect()[0][0] == "SMALL"
        assert df_transformed.filter("product_id = 3").select("size_class").collect()[0][0] == "SMALL"
        assert df_transformed.filter("product_id = 4").select("size_class").collect()[0][0] == "LARGE"
        print("✓ Size class mapping passed")
    
    def test_product_deduplication(self, spark):
        """Verify product dedup keeps latest ingestion."""
        products = [
            (1, "Sofa", 1500.0, "2025-01-01T08:00:00"),
            (1, "Sofa Deluxe", 1600.0, "2025-01-01T10:00:00"),  # Later - should win
            (2, "Lamp", 50.0, "2025-01-01T10:00:00"),
        ]
        df = spark.createDataFrame(products, ["product_id", "title", "price", "ingested_at"])
        
        # Dedup logic
        df_deduped = (df
                      .withColumn("rn", F.row_number().over(
                          Window.partitionBy("product_id").orderBy(F.desc("ingested_at"))
                      ))
                      .filter("rn = 1")
                      .select("product_id", "title", "price"))
        
        # Verify product 1 has the latest title
        product_1 = df_deduped.filter("product_id = 1").select("title").collect()[0][0]
        assert product_1 == "Sofa Deluxe", f"Expected 'Sofa Deluxe', got '{product_1}'"
        assert df_deduped.count() == 2, "Should have 2 unique products"
        print("✓ Product deduplication passed")
    
    def test_null_product_id_filtered(self, spark):
        """Ensure products with null IDs are filtered out."""
        products = [
            (1, "Sofa", 1500.0),
            (None, "Ghost Product", 100.0),  # Should be filtered
            (2, "Lamp", 50.0),
        ]
        df = spark.createDataFrame(products, 
                                    StructType([
                                        StructField("product_id", IntegerType(), True),
                                        StructField("title", StringType(), True),
                                        StructField("price", DoubleType(), True),
                                    ]))
        
        # Filter nulls (same as WHERE clause in SQL)
        df_clean = df.filter("product_id IS NOT NULL")
        
        assert df_clean.count() == 2, "Should filter out null product_id"
        assert df_clean.filter("product_id IS NULL").count() == 0
        print("✓ Null product_id filter passed")


class TestCustomerTransform:
    """Test silver.dim_customers_api logic."""
    
    def test_full_name_concatenation(self, spark):
        """Verify first_name + last_name concatenation."""
        customers = [
            (1, "John", "Doe"),
            (2, "Jane", "Smith"),
        ]
        df = spark.createDataFrame(customers, ["customer_id", "first_name", "last_name"])
        
        # Apply transformation
        df_transformed = df.withColumn("customer_name", 
                                       F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
        
        # Assert
        name_1 = df_transformed.filter("customer_id = 1").select("customer_name").collect()[0][0]
        assert name_1 == "John Doe", f"Expected 'John Doe', got '{name_1}'"
        print("✓ Full name concatenation passed")
    
    def test_null_customer_id_filtered(self, spark):
        """Ensure customers with null IDs are filtered out."""
        customers = [
            (1, "John", "Doe", "john@example.com"),
            (None, "Ghost", "User", "ghost@example.com"),  # Should be filtered
        ]
        df = spark.createDataFrame(customers, 
                                    StructType([
                                        StructField("customer_id", IntegerType(), True),
                                        StructField("first_name", StringType(), True),
                                        StructField("last_name", StringType(), True),
                                        StructField("email", StringType(), True),
                                    ]))
        
        df_clean = df.filter("customer_id IS NOT NULL")
        
        assert df_clean.count() == 1, "Should filter out null customer_id"
        print("✓ Null customer_id filter passed")


class TestDataQualityGates:
    """Test data quality checks that should fail/alert in production."""
    
    def test_detect_duplicate_keys(self, spark):
        """Test that we can detect duplicate primary keys."""
        data = [
            (1, "Product A"),
            (1, "Product A Duplicate"),  # Duplicate key
            (2, "Product B"),
        ]
        df = spark.createDataFrame(data, ["product_id", "title"])
        
        # Quality check: find duplicates
        duplicates = (df
                      .groupBy("product_id")
                      .count()
                      .filter("count > 1"))
        
        # This test verifies we CAN detect duplicates (would be used in a quality gate)
        assert duplicates.count() == 1, "Should detect 1 duplicate key"
        duplicate_id = duplicates.select("product_id").collect()[0][0]
        assert duplicate_id == 1, "Should identify product_id 1 as duplicate"
        print("✓ Duplicate key detection passed")
    
    def test_detect_orphan_foreign_keys(self, spark):
        """Test detection of orphan foreign keys (referential integrity check)."""
        # Transactions
        transactions = [
            (1, 101),  # Valid customer
            (2, 102),  # Valid customer
            (3, 999),  # Orphan - customer doesn't exist
        ]
        df_transactions = spark.createDataFrame(transactions, ["transaction_id", "customer_id"])
        
        # Customers
        customers = [
            (101, "Customer A"),
            (102, "Customer B"),
            # 999 is missing
        ]
        df_customers = spark.createDataFrame(customers, ["customer_id", "customer_name"])
        
        # Find orphans
        df_orphans = (df_transactions
                      .join(df_customers, "customer_id", "left_anti"))  # Anti-join finds orphans
        
        assert df_orphans.count() == 1, "Should detect 1 orphan transaction"
        orphan_id = df_orphans.select("customer_id").collect()[0][0]
        assert orphan_id == 999, "Should identify customer_id 999 as orphan"
        print("✓ Orphan foreign key detection passed")
    
    def test_date_range_validation(self, spark):
        """Test that dates are within expected ranges."""
        from datetime import date as dt_date
        
        data = [
            (1, dt_date(2025, 1, 1)),   # Valid
            (2, dt_date(2025, 6, 15)),  # Valid
            (3, dt_date(1900, 1, 1)),   # Invalid - too old
            (4, dt_date(2030, 1, 1)),   # Invalid - future
        ]
        df = spark.createDataFrame(data, ["id", "transaction_date"])
        
        # Quality check: dates between 2020 and 2026
        df_invalid = df.filter(
            "transaction_date < '2020-01-01' OR transaction_date > '2026-12-31'"
        )
        
        assert df_invalid.count() == 2, "Should detect 2 invalid dates"
        print("✓ Date range validation passed")


# Integration test (optional - would require actual tables)
@pytest.mark.integration
class TestEndToEndTransform:
    """
    Integration tests that would run against actual bronze/silver tables.
    Mark as @pytest.mark.integration to skip in unit test runs.
    
    Run with: pytest tests/test_transforms.py -v -m integration
    """
    
    def test_bronze_to_silver_fx_pipeline(self, spark):
        """
        Full pipeline test: bronze.fx_rates_raw → silver.fx_rates_daily
        Requires actual tables to exist (run in Databricks environment).
        """
        pytest.skip("Integration test - requires Databricks environment")
        
        # This would run the full SQL from 08_silver_api_transform.sql
        # and validate output
    
    def test_silver_to_gold_join_pipeline(self, spark):
        """
        Full pipeline test: silver tables → gold.baskets_enriched
        Requires actual tables and join config.
        """
        pytest.skip("Integration test - requires Databricks environment")


if __name__ == "__main__":
    # Allow running tests directly for debugging
    pytest.main([__file__, "-v", "--tb=short"])

