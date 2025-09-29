
# Databricks notebook source
# MAGIC %md
# MAGIC ## 02_bronze_load
# MAGIC Load CSVs to Delta bronze tables. In production use Auto Loader; here we use simple reads for demo.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

repo_root = spark.sql("SELECT regexp_replace(current_user(), '@.*', '')").collect()[0][0]  # placeholder

# Infer path from notebook context if running in Repo
import os
from pathlib import Path
nb_path = os.getcwd()
repo_dir = "/Workspace" if "/Workspace" in nb_path else "/"
# For demo, adjust to your repo Files path
data_path = "../data_seed"  # edit if needed

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
