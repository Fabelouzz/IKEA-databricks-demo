# Databricks notebook source
# MAGIC %md
# MAGIC ## 05_ml_propensity - Restaurant Conversion Model
# MAGIC Predict which anchor customers will visit the restaurant based on basket behavior.
# MAGIC **Business Use**: Send restaurant coupon at checkout to high-propensity customers.

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# COMMAND ----------

# Load data
tx = spark.table("bronze.attach_transactions")
dim = spark.table("silver.dim_products")
rest = spark.table("bronze.attach_restaurant")
loyalty = spark.table("bronze.attach_loyalty")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Feature Engineering
# MAGIC Create behavioral features from basket composition

# COMMAND ----------

# Build anchor baskets with features
anchor_receipts = (
    tx.join(dim, "product_id")
    .filter(F.col("size_class") == "LARGE")
    .select("receipt_id", "store_id", "ts", "loyalty_id", "family")
    .withColumn("date", F.to_date("ts"))
    .withColumn("hour", F.hour("ts"))
    .withColumn("dow", F.dayofweek("ts"))
    .dropDuplicates(["receipt_id"])
)

# Basket-level features
basket_features = (
    tx.join(dim, "product_id")
    .groupBy("receipt_id")
    .agg(
        F.sum(F.col("price") * F.col("qty")).alias("basket_value"),
        F.sum("qty").alias("item_count"),
        F.sum(F.when(F.col("size_class") == "SMALL", 1).otherwise(0)).alias("addon_count"),
        F.sum(F.when(F.col("family") == "DECOR", 1).otherwise(0)).alias("decor_count"),
        F.sum(F.when(F.col("family") == "KITCHENWARE", 1).otherwise(0)).alias("kitchen_count"),
        F.sum(F.when(F.col("family") == "LAMPS", 1).otherwise(0)).alias("lamp_count"),
        F.countDistinct("family").alias("family_diversity")
    )
)

# Restaurant target (same day)
restaurant_target = (
    rest.withColumn("date", F.to_date("ts"))
    .select("loyalty_id", "date")
    .dropDuplicates()
    .withColumn("visited_restaurant", F.lit(1))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Features and Create Dataset

# COMMAND ----------

# Combine features
dataset = (
    anchor_receipts
    .join(basket_features, "receipt_id")
    .join(loyalty, "loyalty_id")
    .join(restaurant_target, ["loyalty_id", "date"], "left")
    .fillna({"visited_restaurant": 0})
    .withColumn("is_weekend", F.col("dow").isin([1, 7]).cast("int"))
    .withColumn("is_afternoon", (F.col("hour") >= 14).cast("int"))
    .select(
        "receipt_id",
        "basket_value",
        "item_count",
        "addon_count",
        "decor_count",
        "kitchen_count",
        "lamp_count",
        "family_diversity",
        "household_size",
        "home_distance_km",
        "is_weekend",
        "is_afternoon",
        "visited_restaurant"
    )
)

# Show class balance
print("Class Distribution:")
dataset.groupBy("visited_restaurant").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train/Test Split and Model Training

# COMMAND ----------

# Split data
train, test = dataset.randomSplit([0.7, 0.3], seed=42)

# Feature vector
feature_cols = [
    "basket_value", "item_count", "addon_count", 
    "decor_count", "kitchen_count", "lamp_count", "family_diversity",
    "household_size", "home_distance_km", 
    "is_weekend", "is_afternoon"
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# Random Forest Classifier (better than logistic regression for this)
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="visited_restaurant",
    numTrees=50,
    maxDepth=8,
    seed=42
)

pipeline = Pipeline(stages=[assembler, rf])

# COMMAND ----------

# MAGIC %md
# MAGIC ### MLflow Tracking

# COMMAND ----------

with mlflow.start_run(run_name="restaurant_propensity_rf"):
    # Train model
    model = pipeline.fit(train)
    
    # Predictions
    predictions = model.transform(test)
    
    # Evaluate AUC
    evaluator = BinaryClassificationEvaluator(
        labelCol="visited_restaurant",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    auc = evaluator.evaluate(predictions)
    
    # Calculate confusion matrix and classification metrics
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    
    # Accuracy
    accuracy_eval = MulticlassClassificationEvaluator(
        labelCol="visited_restaurant",
        predictionCol="prediction",
        metricName="accuracy"
    )
    accuracy = accuracy_eval.evaluate(predictions)
    
    # Precision
    precision_eval = MulticlassClassificationEvaluator(
        labelCol="visited_restaurant",
        predictionCol="prediction",
        metricName="weightedPrecision"
    )
    precision = precision_eval.evaluate(predictions)
    
    # Recall
    recall_eval = MulticlassClassificationEvaluator(
        labelCol="visited_restaurant",
        predictionCol="prediction",
        metricName="weightedRecall"
    )
    recall = recall_eval.evaluate(predictions)
    
    # F1 Score
    f1_eval = MulticlassClassificationEvaluator(
        labelCol="visited_restaurant",
        predictionCol="prediction",
        metricName="f1"
    )
    f1 = f1_eval.evaluate(predictions)
    
    # Confusion Matrix
    pred_and_labels = predictions.select("prediction", "visited_restaurant").rdd
    metrics_rdd = pred_and_labels.map(lambda x: (float(x[0]), float(x[1])))
    
    # Calculate confusion matrix components
    tp = predictions.filter((F.col("prediction") == 1) & (F.col("visited_restaurant") == 1)).count()
    tn = predictions.filter((F.col("prediction") == 0) & (F.col("visited_restaurant") == 0)).count()
    fp = predictions.filter((F.col("prediction") == 1) & (F.col("visited_restaurant") == 0)).count()
    fn = predictions.filter((F.col("prediction") == 0) & (F.col("visited_restaurant") == 1)).count()
    
    # Log metrics
    mlflow.log_metric("auc", auc)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.log_metric("f1_score", f1)
    mlflow.log_metric("train_count", train.count())
    mlflow.log_metric("test_count", test.count())
    
    # Log confusion matrix components
    mlflow.log_metric("true_positives", tp)
    mlflow.log_metric("true_negatives", tn)
    mlflow.log_metric("false_positives", fp)
    mlflow.log_metric("false_negatives", fn)
    
    # Log model
    mlflow.spark.log_model(model, "restaurant_propensity_model")
    
    # Feature importance
    rf_model = model.stages[-1]
    importance = rf_model.featureImportances
    
    feature_importance = list(zip(feature_cols, importance.toArray()))
    feature_importance.sort(key=lambda x: x[1], reverse=True)
    
    # Print results
    print(f"\n✅ Model Performance:")
    print(f"   AUC: {auc:.4f}")
    print(f"   Accuracy: {accuracy:.4f}")
    print(f"   Precision: {precision:.4f}")
    print(f"   Recall: {recall:.4f}")
    print(f"   F1 Score: {f1:.4f}")
    
    print(f"\n📊 Confusion Matrix:")
    print(f"                 Predicted")
    print(f"               No      Yes")
    print(f"   Actual No   {tn:5d}   {fp:5d}")
    print(f"          Yes  {fn:5d}   {tp:5d}")
    
    print("\nTop 5 Feature Importances:")
    for feat, imp in feature_importance[:5]:
        print(f"  {feat}: {imp:.4f}")
        mlflow.log_metric(f"importance_{feat}", imp)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Insights: High vs Low Propensity Customers

# COMMAND ----------

# Add propensity score (extract probability for class 1 from Vector)
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.ml.linalg import Vector, VectorUDT

# UDF to extract probability of positive class (index 1)
def extract_prob(v):
    try:
        return float(v[1])  # probability of class 1 (visited_restaurant=1)
    except:
        return 0.0

extract_prob_udf = udf(extract_prob, DoubleType())

predictions_with_score = predictions.withColumn(
    "propensity_score", 
    extract_prob_udf(F.col("probability"))
)

# High propensity customers (top 20%)
high_propensity_threshold = predictions_with_score.approxQuantile(
    "propensity_score", [0.8], 0.01
)[0]

print(f"\nHigh Propensity Threshold (Top 20%): {high_propensity_threshold:.3f}")

high_prop = predictions_with_score.filter(
    F.col("propensity_score") >= high_propensity_threshold
)

low_prop = predictions_with_score.filter(
    F.col("propensity_score") < high_propensity_threshold
)

print("\n📊 High Propensity Customers:")
high_prop.select(
    F.mean("basket_value").alias("avg_basket_value"),
    F.mean("item_count").alias("avg_items"),
    F.mean("addon_count").alias("avg_addons"),
    F.mean("visited_restaurant").alias("actual_conversion_rate")
).show()

print("📊 Low Propensity Customers:")
low_prop.select(
    F.mean("basket_value").alias("avg_basket_value"),
    F.mean("item_count").alias("avg_items"),
    F.mean("addon_count").alias("avg_addons"),
    F.mean("visited_restaurant").alias("actual_conversion_rate")
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Predictions for Dashboard
# COMMAND ----------

predictions_with_score.select(
    "receipt_id",
    "propensity_score",
    "visited_restaurant",
    "basket_value",
    "item_count"
).write.mode("overwrite").saveAsTable("gold.restaurant_propensity_scores")

print("✅ Saved predictions to gold.restaurant_propensity_scores")