# Databricks notebook source
# MAGIC %md
# MAGIC ## 06_ml_addon_propensity - Category-Level Add-on Recommendations
# MAGIC Predict which add-on categories a customer will buy based on their anchor purchase.
# MAGIC **Business Use**: Targeted recommendations, staff guidance, app notifications.

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# COMMAND ----------

# Load data
tx = spark.table("bronze.attach_transactions")
dim = spark.table("silver.dim_products")
loyalty = spark.table("bronze.attach_loyalty")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Training Data
# MAGIC For each anchor receipt, create binary labels for each small category

# COMMAND ----------

# Get anchor receipts with their anchor family
anchor_receipts = (
    tx.join(dim, "product_id")
    .filter(F.col("size_class") == "LARGE")
    .select("receipt_id", "loyalty_id", F.col("family").alias("anchor_family"))
    .dropDuplicates(["receipt_id"])
)

# Get which small categories each receipt has
small_categories = ["CHAIRS", "LAMPS", "TEXTILES", "KITCHENWARE", "DECOR", "STORAGE"]

# Create one row per receipt with binary flags for each category
receipt_addon_flags = anchor_receipts

for category in small_categories:
    has_category = (
        tx.join(dim, "product_id")
        .filter(F.col("family") == category)
        .select("receipt_id", F.lit(1).alias(f"has_{category.lower()}"))
        .dropDuplicates()
    )
    receipt_addon_flags = receipt_addon_flags.join(
        has_category, "receipt_id", "left"
    ).fillna({f"has_{category.lower()}": 0})

# Add customer features
receipt_addon_flags = receipt_addon_flags.join(loyalty, "loyalty_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Diagnostics - Check for Variance

# COMMAND ----------

# Check class balance for each category
print("📊 Category Prevalence by Anchor Type:\n")
for category in small_categories:
    col_name = f"has_{category.lower()}"
    
    # Overall prevalence
    overall = receipt_addon_flags.agg(F.mean(col_name)).collect()[0][0]
    
    # By anchor family
    by_anchor = (receipt_addon_flags
                 .groupBy("anchor_family")
                 .agg(F.mean(col_name).alias("rate"), F.count("*").alias("count"))
                 .orderBy("anchor_family"))
    
    print(f"\n{category} (Overall: {overall:.1%}):")
    by_anchor.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train One Model Per Category (Binary Classifiers)

# COMMAND ----------

# Function to train a model for one category
def train_category_model(category_name, target_col):
    """Train a propensity model for one add-on category"""
    
    # Prepare features - ONLY use anchor_family (the only meaningful feature!)
    feature_data = receipt_addon_flags.select(
        "receipt_id",
        "anchor_family",
        target_col
    )
    
    # Use OneHotEncoding for anchor_family (more explicit than StringIndexer)
    indexer = StringIndexer(
        inputCol="anchor_family",
        outputCol="anchor_idx",
        handleInvalid="keep"
    )
    
    encoder = OneHotEncoder(
        inputCol="anchor_idx",
        outputCol="anchor_vec"
    )
    
    assembler = VectorAssembler(
        inputCols=["anchor_vec"],
        outputCol="features"
    )
    
    # Use RandomForest instead of LogisticRegression (handles non-linear patterns better)
    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol=target_col,
        numTrees=30,
        maxDepth=5,
        seed=42
    )
    
    pipeline = Pipeline(stages=[indexer, encoder, assembler, rf])
    
    # Train/test split
    train, test = feature_data.randomSplit([0.7, 0.3], seed=42)
    
    # Train
    model = pipeline.fit(train)
    
    # Evaluate AUC
    predictions = model.transform(test)
    evaluator = BinaryClassificationEvaluator(
        labelCol=target_col,
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    auc = evaluator.evaluate(predictions)
    
    # Classification metrics
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    
    accuracy = MulticlassClassificationEvaluator(
        labelCol=target_col, predictionCol="prediction", metricName="accuracy"
    ).evaluate(predictions)
    
    precision = MulticlassClassificationEvaluator(
        labelCol=target_col, predictionCol="prediction", metricName="weightedPrecision"
    ).evaluate(predictions)
    
    recall = MulticlassClassificationEvaluator(
        labelCol=target_col, predictionCol="prediction", metricName="weightedRecall"
    ).evaluate(predictions)
    
    f1 = MulticlassClassificationEvaluator(
        labelCol=target_col, predictionCol="prediction", metricName="f1"
    ).evaluate(predictions)
    
    # Confusion matrix
    tp = predictions.filter((F.col("prediction") == 1) & (F.col(target_col) == 1)).count()
    tn = predictions.filter((F.col("prediction") == 0) & (F.col(target_col) == 0)).count()
    fp = predictions.filter((F.col("prediction") == 1) & (F.col(target_col) == 0)).count()
    fn = predictions.filter((F.col("prediction") == 0) & (F.col(target_col) == 1)).count()
    
    # Get baseline (% who buy this category)
    baseline_rate = feature_data.agg(F.mean(target_col)).collect()[0][0]
    
    # Calculate lift over baseline
    # For top 20% predicted, what's the actual rate?
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType
    
    def extract_prob(v):
        try:
            return float(v[1])
        except:
            return 0.0
    
    extract_prob_udf = udf(extract_prob, DoubleType())
    
    preds_with_prob = predictions.withColumn("prob", extract_prob_udf(F.col("probability")))
    threshold = preds_with_prob.approxQuantile("prob", [0.8], 0.01)[0]
    top_20 = preds_with_prob.filter(F.col("prob") >= threshold)
    top_20_rate = top_20.agg(F.mean(target_col)).collect()[0][0] if top_20.count() > 0 else baseline_rate
    lift = top_20_rate / baseline_rate if baseline_rate > 0 else 1.0
    
    return model, auc, baseline_rate, lift, accuracy, precision, recall, f1, tp, tn, fp, fn

# COMMAND ----------

# MAGIC %md
# MAGIC ### Train All Category Models with MLflow

# COMMAND ----------

mlflow.set_experiment("/Shared/addon_propensity_models")

results = []

for category in small_categories:
    target_col = f"has_{category.lower()}"
    
    with mlflow.start_run(run_name=f"propensity_{category}"):
        model, auc, baseline, lift, accuracy, precision, recall, f1, tp, tn, fp, fn = train_category_model(category, target_col)
        
        # Log metrics
        mlflow.log_metric("auc", auc)
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("baseline_rate", baseline)
        mlflow.log_metric("lift_top_20", lift)
        mlflow.log_metric("true_positives", tp)
        mlflow.log_metric("true_negatives", tn)
        mlflow.log_metric("false_positives", fp)
        mlflow.log_metric("false_negatives", fn)
        mlflow.log_param("category", category)
        mlflow.log_param("model_type", "RandomForest")
        mlflow.log_param("features", "anchor_family_onehot")
        
        # Log model
        mlflow.spark.log_model(model, f"{category.lower()}_propensity_model")
        
        results.append({
            "category": category,
            "auc": auc,
            "accuracy": accuracy,
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "baseline_rate": baseline,
            "lift": lift
        })
        
        print(f"✅ {category}: AUC={auc:.4f}, Acc={accuracy:.3f}, Prec={precision:.3f}, Rec={recall:.3f}, F1={f1:.3f}")
        print(f"   Confusion Matrix: TP={tp}, TN={tn}, FP={fp}, FN={fn}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Performance Summary

# COMMAND ----------

import pandas as pd

results_df = pd.DataFrame(results)
results_df = results_df.sort_values("auc", ascending=False)

print("\n📊 Model Performance Summary:\n")
print(results_df.to_string(index=False))

# Save to Delta for dashboard (with schema migration for new columns)
spark.createDataFrame(results_df).write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    "gold.addon_propensity_model_performance"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Example: Recommendations for SOFAS
# MAGIC Show top recommended add-on categories when someone buys a sofa

# COMMAND ----------

# Get SOFA receipts
sofa_receipts = receipt_addon_flags.filter(F.col("anchor_family") == "SOFAS")

print(f"\n📦 Total SOFA purchases: {sofa_receipts.count()}")

# Show actual attachment rates for each category
print("\n🎯 Actual Attachment Rates for SOFA Buyers:\n")
for category in small_categories:
    col_name = f"has_{category.lower()}"
    rate = sofa_receipts.agg(F.mean(col_name)).collect()[0][0]
    print(f"  {category}: {rate*100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Cross-Sell Recommendations

# COMMAND ----------

# For each anchor family, rank categories by attachment rate
recommendations = []

for anchor in ["SOFAS", "BEDS", "WARDROBES", "TABLES"]:
    anchor_data = receipt_addon_flags.filter(F.col("anchor_family") == anchor)
    
    category_rates = []
    for category in small_categories:
        col_name = f"has_{category.lower()}"
        rate = anchor_data.agg(F.mean(col_name)).collect()[0][0]
        category_rates.append({"category": category, "rate": rate})
    
    # Sort by rate
    category_rates.sort(key=lambda x: x["rate"], reverse=True)
    
    recommendations.append({
        "anchor_family": anchor,
        "top_1": category_rates[0]["category"],
        "top_1_rate": category_rates[0]["rate"],
        "top_2": category_rates[1]["category"],
        "top_2_rate": category_rates[1]["rate"],
        "top_3": category_rates[2]["category"],
        "top_3_rate": category_rates[2]["rate"]
    })

rec_df = pd.DataFrame(recommendations)
print("\n🎁 Top 3 Add-on Recommendations by Anchor:\n")
print(rec_df.to_string(index=False))

# Save to Delta
spark.createDataFrame(rec_df).write.mode("overwrite").saveAsTable(
    "gold.addon_recommendations"
)

print("\n✅ Saved recommendations to gold.addon_recommendations")
