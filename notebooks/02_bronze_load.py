
# Databricks notebook source
# MAGIC %md
# MAGIC ## 02_bronze_load
# MAGIC Load CSVs to Delta bronze tables. In production use Auto Loader; here we use simple reads for demo.

# COMMAND ----------

from pathlib import Path


def resolve_repo_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        try:
            nb_path = (dbutils.notebook.entry_point.getDbutils()  # type: ignore[name-defined]
                       .notebook().getContext().notebookPath().get())
            workspace_path = Path("/Workspace") / nb_path.lstrip("/")
            # Notebook path usually: /Workspace/Repos/<user>/<repo>/files/notebooks/02_...
            # Take two parents to land in .../files
            return workspace_path.parents[1]
        except Exception:
            return Path.cwd().resolve()

repo_root = resolve_repo_root()
if 'dbutils' in globals():
    data_path = f"dbfs:{repo_root.as_posix()}/data_seed"
else:
    data_path = (repo_root / "data_seed").as_posix()

print(f"📂 Reading seed data from: {data_path}")

# Create schemas
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

# Verify files exist and show sizes
try:
    files = dbutils.fs.ls(data_path)
    print(f"\n📁 CSV files found:")
    for f in files:
        print(f"   {f.name}: {f.size:,} bytes")
except:
    print("⚠️  Could not list files (running locally?)")

# COMMAND ----------

transactions = (spark.read.option("header", True).csv(f"{data_path}/transactions.csv"))
products = (spark.read.option("header", True).csv(f"{data_path}/products.csv"))
loyalty = (spark.read.option("header", True).csv(f"{data_path}/loyalty.csv"))
restaurant = (spark.read.option("header", True).csv(f"{data_path}/restaurant.csv"))
campaigns = (spark.read.option("header", True).csv(f"{data_path}/campaigns.csv"))

# Ensure types
from pyspark.sql.functions import col, to_timestamp
transactions = (transactions
    .withColumn("receipt_id", col("receipt_id").cast("long"))
    .withColumn("store_id", col("store_id").cast("int"))
    .withColumn("ts", to_timestamp("ts"))
    .withColumn("loyalty_id", col("loyalty_id").cast("int"))
    .withColumn("product_id", col("product_id").cast("int"))
    .withColumn("qty", col("qty").cast("int"))
)

products = (products
    .withColumn("product_id", col("product_id").cast("int"))
    .withColumn("price", col("price").cast("double"))
    .withColumn("margin", col("margin").cast("double"))
)

loyalty = (loyalty
    .withColumn("loyalty_id", col("loyalty_id").cast("int"))
    .withColumn("home_distance_km", col("home_distance_km").cast("double"))
    .withColumn("household_size", col("household_size").cast("int"))
)

restaurant = (restaurant
    .withColumn("order_id", col("order_id").cast("long"))
    .withColumn("store_id", col("store_id").cast("int"))
    .withColumn("ts", to_timestamp("ts"))
    .withColumn("loyalty_id", col("loyalty_id").cast("int"))
    .withColumn("basket_value", col("basket_value").cast("double"))
)

# COMMAND ----------

transactions.write.mode("overwrite").saveAsTable("bronze.attach_transactions")
products.write.mode("overwrite").saveAsTable("bronze.attach_products")
loyalty.write.mode("overwrite").saveAsTable("bronze.attach_loyalty")
restaurant.write.mode("overwrite").saveAsTable("bronze.attach_restaurant")
campaigns.write.mode("overwrite").saveAsTable("bronze.attach_campaigns")

display(spark.table("bronze.attach_transactions").limit(5))
