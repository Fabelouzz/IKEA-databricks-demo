
# Databricks notebook source
# MAGIC %md
# MAGIC ## 03_silver_transform
# MAGIC Clean and model baskets, anchor vs add-on, and same-day restaurant joins.

# COMMAND ----------

from pyspark.sql import functions as F, Window as W

tx = spark.table("bronze.attach_transactions")
prod = spark.table("bronze.attach_products")
rest = spark.table("bronze.attach_restaurant")
loy = spark.table("bronze.attach_loyalty")

# Dim products
dim_products = prod.select("product_id","family","size_class","sku","price","margin")
dim_products.write.mode("overwrite").saveAsTable("silver.dim_products")

# Basket with product attributes
b = (tx.join(dim_products, "product_id")
       .withColumn("date", F.to_date("ts"))
       .withColumn("is_anchor", F.col("size_class")=="LARGE")
       .withColumn("is_addon", F.col("size_class")=="SMALL")
   )

# Anchor receipts
anchors = (b.filter("is_anchor")
             .select("receipt_id","store_id","date","loyalty_id")
             .dropDuplicates())
anchors.createOrReplaceTempView("anchors")

# Add-ons within 120 minutes of anchor by same loyalty_id and date
# First, compute min anchor ts per receipt
anchor_time = (b.filter("is_anchor")
                 .groupBy("receipt_id")
                 .agg(F.min("ts").alias("anchor_ts"))
              )

cand = (b.filter("is_addon")
          .join(anchor_time, "receipt_id")
          .withColumn("after_anchor_min", F.col("ts").cast("long") - F.col("anchor_ts").cast("long"))
          .withColumn("after_anchor_min", F.col("after_anchor_min")/60.0)
          .filter("after_anchor_min >= 0 and after_anchor_min <= 120")
       )

# Flag receipts that attached
attach_flag = (cand.select("receipt_id").dropDuplicates()
                 .withColumn("attached", F.lit(1)))

baskets = (anchors.join(attach_flag, "receipt_id", "left")
                 .withColumn("attached", F.coalesce("attached", F.lit(0))))

baskets.write.mode("overwrite").saveAsTable("silver.baskets")

# Restaurant same day for anchor customers
rest_day = (rest.withColumn("date", F.to_date("ts"))
                 .select("loyalty_id","date","order_id","basket_value")
          )
rest_day.write.mode("overwrite").saveAsTable("silver.restaurant_day")

display(spark.table("silver.baskets").groupBy("attached").count())
