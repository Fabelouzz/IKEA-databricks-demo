# Interview Questions Answered by 09_metadata_joins.py

## Overview

The `09_metadata_joins.py` notebook demonstrates a **metadata-driven join architecture** that directly answers several key interview questions about scalability, architecture, and code design in data engineering.

---

## ✅ Questions Directly Answered

### 1. **Joining Multiple Tables with Business Logic**

**Question:** *"You need to join four tables with business logic in PySpark. What approach would you take to write those joins?"*

**Answer Demonstrated in Notebook:**

The notebook implements a **generic join engine** that:

1. **Loads a base table** with filtering
2. **Applies multiple joins sequentially** based on configuration
3. **Adds business KPIs** after joins complete

**Code Implementation:**
```python
def apply_metadata_joins(config):
    # Load base table
    df_base = spark.table(config["base"]["table"])
    df_base = df_base.filter(config["base"]["filter"])
    
    # Apply joins sequentially
    for join_cfg in config["joins"]:
        df_join = spark.table(join_cfg["table"])
        result_df = result_df.join(df_join, on=join_cfg["on"], how=join_cfg["type"])
    
    # Add business KPIs
    result_df = result_df.withColumn("price_sek", F.col("price") * F.col("fx_sek_rate"))
    # ... more KPIs
    
    return result_df
```

**Business Logic Included:**
- Multi-currency pricing (EUR → SEK, EUR → USD)
- Inventory value calculations
- Customer age group segmentation
- Synthetic join conditions for demo purposes

---

### 2. **Scalable Design: Adding 10-20+ Tables with Minimal Code Changes** ⭐

**Question:** *"How would you design the code so that you can keep adding many tables (10, 20, …) and have the joins handled with minimal code changes (framework/structure/feature)?"*

**Answer Demonstrated in Notebook:**

**ARCHITECTURE: Configuration-Driven Joins**

The notebook separates **what to join** (configuration) from **how to join** (code):

**Python Code (Generic - Never Changes):**
```python
# This loop works for 3 joins, 20 joins, or 100 joins - NO CODE CHANGES
for idx, join_cfg in enumerate(config["joins"], 1):
    join_table = join_cfg["table"]
    join_alias = join_cfg["alias"]
    join_on = join_cfg["on"]
    join_type = join_cfg.get("type", "left")
    
    # Load and join
    df_join = spark.table(join_table).alias(join_alias)
    if join_cfg.get("broadcast", False):
        df_join = F.broadcast(df_join)
    
    result_df = result_df.join(df_join, on=F.expr(join_on), how=join_type)
    
    # Select specified columns
    if "select" in join_cfg:
        # ... column selection logic ...
```

**YAML Configuration (Add Joins Here - No Python Changes):**
```yaml
joins:
  # Join 1
  - table: silver.dim_customers_api
    alias: cust
    type: left
    on: "base.product_id % 208 = cust.customer_id"
    select: [customer_id, customer_name, age, gender]
    broadcast: true
    
  # Join 2
  - table: silver.fx_rates_daily
    alias: fx_sek
    type: left
    on: "base.product_id % 30 = DAY(fx_sek.as_of_date) AND fx_sek.pair = 'EUR/SEK'"
    select: [rate]
    broadcast: false
    
  # ADD MORE JOINS HERE - NO PYTHON CODE CHANGES NEEDED!
  # - table: silver.new_table
  #   alias: new
  #   ...
```

**Key Benefits:**
- ✅ **Add 100 tables** by editing YAML only
- ✅ **No code changes** to Python logic
- ✅ **Broadcast optimization** configurable per table
- ✅ **Join types** configurable (left, inner, outer)
- ✅ **Column selection** controlled in config
- ✅ **Production pattern** used in real enterprise ETL

---

### 3. **Schema/Config Format for Scalability**

**Question:** *"What kind of schema/config would you create to support that? In which format would you keep this schema so your code reads it and behaves accordingly?"*

**Answer Demonstrated in Notebook:**

**FORMAT: YAML (Human-Readable, Version-Controlled)**

**File:** `config/joins.yml`

**Structure:**
```yaml
# Base table configuration
base:
  table: silver.dim_products_api
  alias: base
  select: [product_id, title, category, brand, price, stock, rating, size_class]
  filter: "price > 0 AND stock > 0"

# Join configurations (array of join definitions)
joins:
  - table: <table_name>
    alias: <short_alias>
    type: <left|inner|outer>
    on: "<join_condition_as_SQL>"
    select: [<columns_to_select>]
    broadcast: <true|false>

# Output configuration
output:
  table: gold.products_analytics_simple
  mode: overwrite
```

**Why YAML?**
1. **Human-readable** - Business analysts can understand it
2. **Version-controlled** - Track changes in Git
3. **Standard format** - Used by Kubernetes, Airflow, dbt
4. **Comments supported** - Document business logic
5. **Type safety** - Lists, booleans, strings are typed
6. **No code compilation** - Just edit and re-run

**Python Loading:**
```python
import yaml

with open("config/joins.yml") as f:
    config = yaml.safe_load(f)

# Now config is a Python dict
for join in config["joins"]:
    print(f"Joining {join['table']} on {join['on']}")
```

**Alternative Formats Considered:**
- ❌ **JSON** - Less readable, no comments
- ❌ **Python dict** - Requires code changes to edit
- ❌ **Database table** - Overkill for join config
- ✅ **YAML** - Best balance of readability and functionality

---

### 4. **Metadata Architecture**

**Question:** *"Have you heard about 'metadata architecture'? Can you explain how it works?"*

**Answer Demonstrated in Notebook:**

**DEFINITION:** Metadata architecture separates **data about data** from **processing logic**.

**Implementation in Notebook:**

**Metadata Layer (YAML Config):**
- Defines table names
- Defines join conditions
- Defines column selections
- Defines broadcast hints
- Documents business logic

**Processing Layer (Python Code):**
- Reads metadata
- Executes joins generically
- Applies transformations
- Validates results

**Benefits:**
1. **Separation of Concerns:** Business logic in config, technical logic in code
2. **Self-Documenting:** Config file serves as pipeline documentation
3. **Governance:** Track what joins to what, and why (via config comments)
4. **Lineage:** Easy to see data flow from config
5. **Scalability:** Add data sources without code changes

**Real-World Applications:**
- **Data catalogs** (Unity Catalog, Alation) - metadata about tables
- **ETL frameworks** (Airflow, dbt) - metadata about pipelines
- **Schema registries** (Confluent) - metadata about Kafka topics
- **This notebook** - metadata about joins

**Example from Notebook:**
```yaml
# Metadata: What this join does and why
- table: silver.dim_customers_api     # What to join
  alias: cust                          # How to reference it
  on: "base.product_id % 208 = cust.customer_id"  # Join logic
  select: [customer_id, customer_name, age, gender]  # What to keep
  broadcast: true                      # Performance hint
```

---

### 5. **Join Problem Diagnosis** (Bonus)

**Question:** *"If there's a problem in a join, how would you understand where it occurs?"*

**Answer Demonstrated Through Debugging:**

The notebook went through multiple iterations to fix join issues:

**Problem 1: Column Drop-Before-Select**
- **Symptom:** `[UNRESOLVED_COLUMN] cust.customer_id cannot be resolved`
- **Diagnosis:** Dropping columns before trying to select them
- **Solution:** Capture existing columns BEFORE join, then select desired columns

**Problem 2: Ambiguous Column References**
- **Symptom:** `[AMBIGUOUS_REFERENCE] category is ambiguous`
- **Diagnosis:** Self-joins creating duplicate column names
- **Solution:** Always alias columns from joins (e.g., `cust_customer_id`)

**Problem 3: Complex Expressions in Self-Joins**
- **Symptom:** Window functions failing on self-joined tables
- **Diagnosis:** Too complex for initial demo
- **Solution:** Simplified to 3 clean joins without self-joins

**Debugging Approach Demonstrated:**
1. **Check column names** after each join
2. **Use explicit aliases** for all tables
3. **Print intermediate results** to understand transformations
4. **Start simple** (3 joins) before scaling up
5. **Test incrementally** - add one join at a time

---

## 📊 Demonstration Results

**Current Implementation:**
- **Base table:** 190 products (filtered)
- **3 joins applied:**
  - Customer demographics (208 customers, broadcast optimized)
  - FX rates EUR→SEK
  - FX rates EUR→USD
- **Result:** 911 enriched rows (cartesian due to synthetic joins)
- **19 columns total:** 8 base + 4 customer + 2 FX + 5 derived KPIs

**Business KPIs Added:**
- Multi-currency pricing (SEK, USD)
- Inventory values in multiple currencies
- Customer age group segmentation

**Scalability Proof:**
```python
# To add 17 more joins (total 20), edit YAML only:
joins:
  # ... existing 3 joins ...
  - table: silver.dim_categories
  - table: silver.dim_suppliers
  - table: silver.fact_sales
  # ... 14 more ...
  
# Python code stays EXACTLY THE SAME
```

---

## 🎯 Key Takeaways for Interviews

### Question: "How do you design scalable join logic?"

**Answer Template:**

> "I use a **metadata-driven architecture** where join configurations are stored in YAML files, and a generic Python engine reads the config and applies joins dynamically.
> 
> For example, in my IKEA demo project, I have a `joins.yml` file that defines:
> - Which tables to join
> - Join conditions
> - Column selections
> - Performance hints (broadcast)
> 
> The Python code loops through this config and applies joins generically. This means I can add 10, 20, or 100 tables by just editing the YAML file - **no code changes needed**.
> 
> This approach:
> - Separates business logic (config) from technical logic (code)
> - Is self-documenting
> - Makes changes traceable in Git
> - Follows enterprise patterns used by dbt and Airflow
> 
> I chose YAML because it's human-readable, supports comments, and is version-controllable. It's better than JSON (no comments) or hardcoded Python dicts (requires code changes)."

### Question: "What is metadata architecture?"

**Answer Template:**

> "Metadata architecture means storing **data about data** separately from processing logic.
> 
> In my join pipeline example:
> - **Metadata:** YAML config describes what tables exist, how to join them, what columns to use
> - **Processing:** Python code reads metadata and executes joins generically
> 
> Benefits:
> - **Governance:** Easy to see what joins to what
> - **Lineage:** Track data flow through configs
> - **Scalability:** Add data sources without code changes
> - **Documentation:** Config serves as pipeline documentation
> 
> Real-world examples include Unity Catalog (metadata about tables), Airflow DAGs (metadata about pipelines), and schema registries (metadata about message formats)."

---

## 📁 Related Files

- **Notebook:** `notebooks/09_metadata_joins.py`
- **Config:** `config/joins.yml`
- **Input Tables:**
  - `silver.dim_products_api` (194 products from DummyJSON)
  - `silver.dim_customers_api` (208 users from DummyJSON)
  - `silver.fx_rates_daily` (~128 FX rates from Frankfurter API)
- **Output Table:** `gold.products_analytics_simple` (911 enriched rows)

---

## 🔗 Connection to Other Questions

While this notebook directly answers the scalability/architecture questions, other notebooks in the project answer different questions:

- **`07_ingest_api_data.py`** - API ingestion, pagination, retry logic
- **`08_silver_api_transform.sql`** - CTEs, primary/foreign keys, data quality
- **`10_delta_time_travel.py`** - Delta Lake time travel, versioning
- **`tests/test_transforms.py`** - Unit testing for data pipelines

---

## 💡 Interview Tips

1. **Show the YAML first** - Interviewers love seeing config-driven architecture
2. **Explain the loop** - Show how one piece of code handles N joins
3. **Mention broadcast hints** - Shows performance awareness
4. **Talk about scalability** - "Add 100 tables by editing YAML only"
5. **Compare to alternatives** - JSON (no comments), hardcoded (not scalable)
6. **Real-world examples** - dbt, Airflow, Kubernetes all use YAML configs

**Demo Script:**
1. Show the YAML config (30 seconds)
2. Explain the Python loop (1 minute)
3. Show how to add a new join (30 seconds)
4. Run the notebook to prove it works (1 minute)
5. Discuss benefits: scalability, maintainability, governance (1 minute)

**Total: 4 minutes** for a complete, impressive demonstration of architectural thinking.

