
# Databricks notebook source
# MAGIC %md
# MAGIC ## 05_ml_propensity
# MAGIC Train a simple propensity model for buying an add-on given an anchor family. Logs to MLflow.

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

tx = spark.table("bronze.attach_transactions")
dim = spark.table("silver.dim_products")

b = (tx.join(dim, "product_id")
       .withColumn("date", F.to_date("ts"))
       .withColumn("is_anchor", F.col("size_class")=="LARGE")
       .withColumn("is_addon", F.col("size_class")=="SMALL")
   )

anchor_family = "SOFAS"
anchor_receipts = (b.filter((F.col("is_anchor")) & (F.col("family")==anchor_family))
                     .select("receipt_id").dropDuplicates())

receipts = (b.select("receipt_id","loyalty_id","store_id","date").dropDuplicates())
features = (receipts
            .join(anchor_receipts.withColumn("target", F.lit(1)), "receipt_id", "left")
            .fillna({"target":0})
            .withColumn("dow", F.dayofweek("date"))
            .withColumn("is_weekend", F.col("dow").isin([1,7]).cast("int"))
)

indexer = StringIndexer(inputCol="store_id", outputCol="store_idx", handleInvalid="keep")
va = VectorAssembler(inputCols=["store_idx","dow","is_weekend"], outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="target")

pipe = Pipeline(stages=[indexer, va, lr])

with mlflow.start_run():
    model = pipe.fit(features)
    mlflow.spark.log_model(model, "model")
    summary = model.stages[-1].summary
    mlflow.log_metric("areaUnderROC", summary.areaUnderROC)
    print("AUC:", summary.areaUnderROC)
