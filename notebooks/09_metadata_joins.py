# Databricks notebook source
# MAGIC %md
# MAGIC ## 09_metadata_joins - Metadata-Driven Join Pipeline
# MAGIC 
# MAGIC **Simple metadata-driven join engine** that demonstrates:
# MAGIC - **3 joins** defined in YAML configuration
# MAGIC - **Broadcast optimization** for small dimensions
# MAGIC - **Pure API data** (DummyJSON + Frankfurter)
# MAGIC - **Multi-currency pricing** (EUR/SEK, EUR/USD)
# MAGIC - **Customer enrichment** (demographics)
# MAGIC 
# MAGIC **Business Value**: Product analytics with customer and FX enrichment

# COMMAND ----------

# Install YAML library if not available
%pip install pyyaml

# COMMAND ----------

import yaml
from pathlib import Path
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Load Join Configuration from YAML

# COMMAND ----------

# PURPOSE: Locate and load the joins.yml config file
# PATTERN: Handles both Databricks workspace and local environments

def resolve_repo_root():
    """Find repo root directory in Databricks or local environment."""
    try:
        # Local: __file__ is defined
        return Path(__file__).resolve().parents[1]
    except NameError:
        # Databricks: Use notebook context to find workspace path
        try:
            nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
            workspace_path = Path("/Workspace") / nb_path.lstrip("/")
            # Notebook path: /Workspace/Repos/<user>/<repo>/notebooks/09_...
            # Go up 1 level to reach repo root
            return workspace_path.parents[1]
        except:
            # Fallback: Current working directory
            return Path.cwd().resolve()

repo_root = resolve_repo_root()
config_path = repo_root / "config" / "joins.yml"

print(f"📂 Repo root: {repo_root}")
print(f"📄 Config path: {config_path}")

# COMMAND ----------

# Load YAML configuration
# PATTERN: Read from DBFS (Databricks) or local filesystem

if 'dbutils' in globals():
    # Databricks: Read from DBFS
    try:
        config_content = dbutils.fs.head(f"dbfs:{config_path.as_posix()}", 100000)
    except:
        # Fallback: Try reading from /Workspace path directly
        with open(config_path) as f:
            config_content = f.read()
else:
    # Local: Read from filesystem
    with open(config_path) as f:
        config_content = f.read()

# Parse YAML into Python dict
config = yaml.safe_load(config_content)

print("✓ Loaded join configuration")
print(f"📊 Joins defined: {len(config['joins'])}")
print(f"🎯 Base table: {config['base']['table']}")
print(f"📈 Output table: {config['output']['table']}")
print(f"\n💡 TIP: Add more joins by editing config/joins.yml - no code changes needed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Generic Join Engine

# COMMAND ----------

# PURPOSE: Apply joins based on YAML config with simple business KPIs
# PATTERN: Metadata-driven architecture - add tables via config, not code

def apply_metadata_joins(config):
    """
    Apply joins based on YAML configuration with simple business KPIs.
    
    FEATURES:
    - Clean join logic (no complex self-joins)
    - Broadcast hash joins for small dimensions
    - Multi-currency pricing
    - Customer demographics enrichment
    
    Args:
        config: Dict loaded from joins.yml
        
    Returns:
        DataFrame: Enriched result with all joins applied
    """
    
    # STEP 1: Load base table
    base_cfg = config["base"]
    base_table = base_cfg["table"]
    base_alias = base_cfg.get("alias", "base")
    
    print(f"\n{'='*60}")
    print(f"LOADING BASE TABLE: {base_table}")
    print(f"{'='*60}")
    
    df_base = spark.table(base_table)
    
    # STEP 2: Apply base filter (if specified)
    if "filter" in base_cfg:
        print(f"  📋 Applying filter: {base_cfg['filter']}")
        df_base = df_base.filter(base_cfg["filter"])
    
    # STEP 3: Select base columns (if specified)
    if "select" in base_cfg:
        print(f"  📊 Selecting {len(base_cfg['select'])} columns")
        df_base = df_base.select(*base_cfg["select"])
    
    # Alias the base table for join conditions
    df_base = df_base.alias(base_alias)
    result_df = df_base
    
    print(f"  ✓ Base loaded: {result_df.count():,} rows")
    
    # STEP 4: Apply joins sequentially
    joins_cfg = config.get("joins", [])
    print(f"\n{'='*60}")
    print(f"APPLYING {len(joins_cfg)} JOINS")
    print(f"{'='*60}")
    
    for idx, join_cfg in enumerate(joins_cfg, 1):
        join_table = join_cfg["table"]
        join_alias = join_cfg["alias"]
        join_type = join_cfg.get("type", "left")
        join_on = join_cfg["on"]
        broadcast = join_cfg.get("broadcast", False)
        
        print(f"\n  [{idx}/{len(joins_cfg)}] {join_table} ({join_type.upper()} JOIN)")
        
        # Load join table
        df_join = spark.table(join_table)
        
        # Alias the join table
        df_join = df_join.alias(join_alias)
        
        # Apply broadcast hint if specified (for small dimensions)
        if broadcast:
            df_join = F.broadcast(df_join)
            print(f"      📡 Broadcast hint applied (small table optimization)")
        
        # Get existing columns BEFORE the join
        existing_cols = result_df.columns
        
        # Perform the join
        result_df = result_df.join(
            df_join,
            on=F.expr(join_on),
            how=join_type
        )
        
        # Build select statement to keep only desired columns from joined table
        if "select" in join_cfg:
            # Build list of columns to keep from existing DataFrame
            base_select = [F.col(col) for col in existing_cols]
            
            # Handle select expressions from the YAML config
            select_exprs = []
            for col_expr in join_cfg["select"]:
                if isinstance(col_expr, str):
                    # Check if it's a complex expression (window function, CASE, AS alias, etc.)
                    # Complex expressions are used as-is since they already have their own alias
                    if any(keyword in col_expr.upper() for keyword in [
                        'COUNT(*)', 'COUNT(', 'AVG(', 'MAX(', 'MIN(', 'SUM(',
                        'PERCENTILE_CONT', 'PERCENTILE_DISC',
                        'CONCAT(', 'CONCAT_WS(',
                        'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
                        'OVER', 'PARTITION', 'ORDER BY',
                        ' AS ',  # Column alias (e.g., "category AS category_name")
                        'DISTINCT'
                    ]):
                        # Complex expression - use as-is (already has alias in YAML)
                        select_exprs.append(F.expr(col_expr))
                    else:
                        # Simple column reference - select and alias to avoid conflicts
                        # Format: join_alias_column_name (e.g., cust_customer_id)
                        select_exprs.append(F.col(f"{join_alias}.{col_expr}").alias(f"{join_alias}_{col_expr}"))
                else:
                    # Complex expression
                    select_exprs.append(F.expr(col_expr))
            
            # Final select: keep all existing columns + add new ones from this join
            result_df = result_df.select(*base_select, *select_exprs)
            print(f"      📊 Selected {len(join_cfg['select'])} new columns")
        
        print(f"      ✓ Joined on: {join_on}")
    
    # STEP 5: Add simple business KPIs
    print(f"\n{'='*60}")
    print("ADDING BUSINESS KPIs")
    print(f"{'='*60}")
    
    # Multi-currency pricing (simple multiplication)
    result_df = result_df.withColumn(
        "price_sek", 
        F.col("price") * F.coalesce(F.col("fx_sek_rate"), F.lit(11.5))
    ).withColumn(
        "price_usd", 
        F.col("price") * F.coalesce(F.col("fx_usd_rate"), F.lit(1.1))
    )
    
    # Inventory value
    result_df = result_df.withColumn(
        "inventory_value_sek",
        F.col("price_sek") * F.col("stock")
    ).withColumn(
        "inventory_value_usd", 
        F.col("price_usd") * F.col("stock")
    )
    
    # Customer age group (simple categorization)
    result_df = result_df.withColumn(
        "customer_age_group",
        F.when(F.col("cust_age") < 30, "YOUNG")
         .when(F.col("cust_age") < 50, "MIDDLE")
         .when(F.col("cust_age") >= 50, "SENIOR")
         .otherwise("UNKNOWN")
    )
    
    print("  ✓ Multi-currency pricing added")
    print("  ✓ Inventory value calculations added")
    print("  ✓ Customer age groups added")
    
    return result_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Execute Join Pipeline

# COMMAND ----------

# Apply the metadata-driven joins with simple business KPIs
print("\n" + "🚀 STARTING JOIN PIPELINE " + "="*50)

df_enriched = apply_metadata_joins(config)

print("\n" + "="*60)
print("✓ ENRICHED DATAFRAME CREATED")
print("="*60)
print(f"  Rows: {df_enriched.count():,}")
print(f"  Columns: {len(df_enriched.columns)}")
print(f"\n📋 Column names:")
for col in df_enriched.columns:
    print(f"  - {col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Write to Gold Layer

# COMMAND ----------

# Write enriched data to gold table
output_cfg = config.get("output", {})
output_table = output_cfg.get("table", "gold.products_analytics_comprehensive")
output_mode = output_cfg.get("mode", "overwrite")

print(f"\n📝 WRITING TO GOLD LAYER")
print(f"  Table: {output_table}")
print(f"  Mode: {output_mode}")

df_enriched.write.mode(output_mode).saveAsTable(output_table)

print(f"  ✓ Written successfully")

# COMMAND ----------

# Verify write
row_count = spark.table(output_table).count()
print(f"\n✅ VERIFICATION")
print(f"  {output_table}: {row_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Business Analytics Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC #### Key Business Metrics Sample

# COMMAND ----------

# Display sample of key business metrics
print("\n📊 SAMPLE BUSINESS METRICS")
print("="*60)

sample_cols = [
    "product_id", "title", "category", "brand", "price", "stock",
    "cust_customer_name", "cust_age", "gender",
    "fx_sek_rate", "fx_usd_rate",
    "price_sek", "price_usd", "inventory_value_sek",
    "customer_age_group"
]

df_enriched.select(*sample_cols).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Category Performance Analysis

# COMMAND ----------

# Category-level aggregations
print("\n📈 CATEGORY PERFORMANCE ANALYSIS")
print("="*60)

category_performance = df_enriched.groupBy("category").agg(
    F.count("*").alias("product_count"),
    F.avg("price").alias("avg_price_eur"),
    F.avg("price_sek").alias("avg_price_sek"),
    F.avg("price_usd").alias("avg_price_usd"),
    F.avg("rating").alias("avg_rating"),
    F.sum("inventory_value_sek").alias("total_inventory_value_sek"),
    F.sum("inventory_value_usd").alias("total_inventory_value_usd")
).orderBy(F.desc("total_inventory_value_sek"))

category_performance.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Brand Performance Analysis

# COMMAND ----------

# Brand-level aggregations
print("\n🏷️ BRAND PERFORMANCE ANALYSIS")
print("="*60)

brand_performance = df_enriched.groupBy("brand").agg(
    F.count("*").alias("product_count"),
    F.avg("price").alias("avg_price_eur"),
    F.avg("rating").alias("avg_rating"),
    F.sum("inventory_value_sek").alias("total_inventory_value_sek"),
    F.countDistinct("category").alias("category_diversity")
).orderBy(F.desc("total_inventory_value_sek"))

brand_performance.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customer Demographics Analysis

# COMMAND ----------

# Customer age group distribution
print("\n👥 CUSTOMER AGE GROUP DISTRIBUTION")
print("="*60)

customer_demographics = df_enriched.groupBy("customer_age_group", "gender").agg(
    F.count("*").alias("product_count"),
    F.avg("price").alias("avg_price_eur"),
    F.avg("inventory_value_sek").alias("avg_inventory_value_sek")
).orderBy("customer_age_group", "gender")

customer_demographics.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top Products by Inventory Value

# COMMAND ----------

# Top products by inventory value
print("\n💰 TOP PRODUCTS BY INVENTORY VALUE")
print("="*60)

top_products = df_enriched.select(
    "product_id", "title", "category", "brand", 
    "price", "stock", "rating",
    "price_sek", "price_usd",
    "inventory_value_sek", "inventory_value_usd",
    "cust_customer_name", "customer_age_group"
).orderBy(F.desc("inventory_value_sek")).limit(15)

top_products.show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Performance Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC #### Query Plan Analysis (Broadcast Joins)

# COMMAND ----------

# Show the physical plan to verify broadcast joins
print("🔍 QUERY PLAN ANALYSIS")
print("="*60)
print("Looking for 'BroadcastHashJoin' to confirm broadcast optimization...")
print("="*60 + "\n")

df_enriched.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schema Verification

# COMMAND ----------

# Verify schema includes all enrichment columns
print("📋 GOLD TABLE SCHEMA")
print("="*60)

gold_schema = spark.table(output_table).schema
for field in gold_schema:
    print(f"  {field.name}: {field.dataType} (nullable={field.nullable})")

print(f"\n✓ Total columns: {len(gold_schema)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Summary & Business Value

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    METADATA-DRIVEN JOINS - SUMMARY                          ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║ ✅ PURE API PIPELINE: DummyJSON + Frankfurter (no legacy data)              ║
║ ✅ METADATA-DRIVEN: 3 joins defined in YAML configuration                   ║
║ ✅ BROADCAST OPTIMIZATION: Small dimensions broadcasted                     ║
║ ✅ MULTI-CURRENCY: EUR/SEK and EUR/USD pricing                              ║
║ ✅ CUSTOMER ENRICHMENT: Demographics joined to products                     ║
║ ✅ BUSINESS KPIs: Inventory values and customer segments                    ║
║                                                                              ║
║ JOINS APPLIED:                                                               ║
║ 1. Customer demographics (208 customers)                                     ║
║ 2. FX rates - EUR to SEK (daily rates)                                      ║
║ 3. FX rates - EUR to USD (daily rates)                                      ║
║                                                                              ║
║ TO EXTEND:                                                                   ║
║ 1. Edit config/joins.yml to add more simple joins                           ║
║ 2. Re-run this notebook to apply changes                                    ║
║ 3. Add more KPIs in apply_metadata_joins() function                         ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing Notes
# MAGIC 
# MAGIC **Expected Results:**
# MAGIC - `gold.products_analytics_simple` created successfully
# MAGIC - 190 rows (products with price > 0 and stock > 0)
# MAGIC - ~15 columns: 8 base + 4 customer + 2 FX + KPIs
# MAGIC - Explain plan shows `BroadcastHashJoin` for customer dimension
# MAGIC 
# MAGIC **Business Value:**
# MAGIC - Multi-currency pricing (EUR, SEK, USD)
# MAGIC - Customer demographics enrichment
# MAGIC - Inventory value calculations
# MAGIC - Simple, maintainable metadata-driven architecture