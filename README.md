# Attach-rate Demo on Databricks

Goal: Show a mini Lakehouse for a retail-like store where large "anchor" purchases drive attach of small add-ons and restaurant visits the same day.

What you get
1. Synthetic datasets for 60 days of activity for a single store.
2. PySpark notebooks for Bronze → Silver → Gold.
3. Optional ML propensity notebook tracked with MLflow.
4. dbt models and tests for data quality on key tables.
5. SQL queries for a small dashboard in Databricks SQL or Power BI.

Quickstart on Databricks
1. Create a new Repo and upload the contents of this project.
2. In a cluster with DBR 14.x+ and Python 3.10, open `notebooks/01_generate_synth_data.py` and run all cells.
3. Run `notebooks/02_bronze_load.py` to load CSV → Delta (bronze schema).
4. Run `notebooks/03_silver_transform.py` to clean and model baskets.
5. Run `notebooks/04_gold_views.sql` in a SQL editor to create views used by dashboards.
6. Optional: run `notebooks/05_ml_propensity.py` to train a simple add-on propensity model and log to MLflow.
7. Open `sql/attach_dashboard.sql` for example queries and KPI definitions.

Lakehouse layout
- bronze.attach_* raw Delta tables
- silver.baskets, silver.restaurant_day, silver.dim_products
- gold.attach_metrics, gold.addon_lift, gold.restaurant_conv

KPI definitions
- Attach rate = share of anchor receipts that include at least one add-on from the recommended set within 120 minutes.
- Cross-sell lift = P(addon | anchor) / P(addon).
- Restaurant conversion = share of anchor customers that also order in the restaurant same day.

Assumptions
- One store_id for demo. Easy to expand to multiple stores.
- Product size_class in {LARGE, SMALL} where LARGE acts as anchor.
- Timezone naive; treat timestamps as local store time.
