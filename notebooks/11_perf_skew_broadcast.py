# Databricks notebook source
# MAGIC %md
# MAGIC ## 11_perf_skew_broadcast
# MAGIC 
# MAGIC Performance tuning demonstration:
# MAGIC - Reproduce data skew (hot key problem)
# MAGIC - Measure impact on Spark stages
# MAGIC - Fix with AQE and broadcast join
# MAGIC - Compare via Spark UI
# MAGIC 
# MAGIC **Business impact**: Faster queries, better resource utilization, cost savings

# COMMAND ----------

from pyspark.sql import functions as F
import time

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create Skewed Dataset (Reproduce Real-World Problem)

# COMMAND ----------

# PURPOSE: Simulate a common production problem - data skew
# SCENARIO: One customer (or store, or product) has 40% of all transactions
# BUSINESS EXAMPLE: Amazon seller with massive volume, or Black Friday sale
# TECHNICAL IMPACT: One Spark task processes 40% of data while others idle

print("🔧 GENERATING SKEWED DATASET")
print("="*60)
print("Simulating: 1 million transactions with heavy skew")
print("Problem: Customer ID 1 has 40% of all transactions (hot key)")
print("="*60 + "\n")

# Normal customers (60% of data, evenly distributed across 1000 customers)
print("Creating normal customer transactions (60% of data)...")
normal_data = spark.range(0, 600_000).select(
    (F.col("id") % 1000).alias("customer_id"),  # Distribute across customer IDs 0-999
    (F.rand() * 1000).cast("double").alias("amount"),
    F.current_date().alias("date")
)

print("✓ Normal data: 600,000 rows across 1,000 customers")

# COMMAND ----------

# Skewed customer (40% of data, ALL go to ONE customer)
print("\nCreating skewed customer transactions (40% of data)...")
skewed_data = spark.range(0, 400_000).select(
    F.lit(1).alias("customer_id"),  # ALL rows go to customer_id = 1 (HOT KEY)
    (F.rand() * 1000).cast("double").alias("amount"),
    F.current_date().alias("date")
)

print("✓ Skewed data: 400,000 rows for customer_id = 1")

# COMMAND ----------

# Combine normal + skewed data
df_transactions_skewed = normal_data.union(skewed_data)

# Cache to ensure consistent timing across runs
df_transactions_skewed.cache()
total_count = df_transactions_skewed.count()  # Force cache

print(f"\n📊 FINAL DATASET:")
print(f"   Total transactions: {total_count:,}")
print(f"   Skew level: 40% in one customer_id")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verify Skew (Data Distribution Analysis)

# COMMAND ----------

# PURPOSE: Prove the skew exists - visualize the problem
# PATTERN: GROUP BY to see distribution across keys

print("🔍 DATA DISTRIBUTION ANALYSIS")
print("="*60)

skew_check = (df_transactions_skewed
              .groupBy("customer_id")
              .count()
              .orderBy(F.desc("count")))

print("Top 10 customers by transaction count:")
display(skew_check.limit(10))

# COMMAND ----------

# Quantify the skew
customer_1_count = skew_check.filter("customer_id = 1").select("count").collect()[0][0]
total = df_transactions_skewed.count()
percentage = (customer_1_count / total) * 100

print(f"\n📊 SKEW METRICS:")
print(f"   Customer 1 transactions: {customer_1_count:,}")
print(f"   Percentage of total: {percentage:.1f}%")
print(f"   Problem: One partition will have {percentage:.0f}% of the data!")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Create Small Customer Dimension (Join Target)

# COMMAND ----------

# PURPOSE: Create a small dimension for joining
# SIZE: 1,000 customers (~10KB) - perfect for broadcast join
# BUSINESS: Customer master data with names, segments, etc.

print("👥 CREATING CUSTOMER DIMENSION")
print("="*60)

df_customers = spark.range(0, 1000).select(
    F.col("id").alias("customer_id"),
    F.concat(F.lit("Customer_"), F.col("id")).alias("customer_name"),
    (F.rand() * 100).cast("int").alias("age"),
    F.when(F.rand() > 0.5, "Premium").otherwise("Standard").alias("segment")
)

# Cache dimension
df_customers.cache()
customer_count = df_customers.count()

print(f"✓ Customers: {customer_count:,}")
print(f"✓ Estimated size: ~10KB (small enough for broadcast)")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. NAIVE JOIN (No Optimization - Show the Problem)

# COMMAND ----------

# PURPOSE: Demonstrate the performance impact of skew
# APPROACH: Disable optimizations to show raw skew impact
# EXPECTATION: Slow join with unbalanced tasks

print("\n" + "🐌 NAIVE JOIN (No Optimizations)")
print("="*60)
print("Configuration:")
print("  - Adaptive Query Execution (AQE): DISABLED")
print("  - Broadcast Join: DISABLED")
print("  - Skew Join Handling: DISABLED")
print("="*60 + "\n")

# Disable all optimizations
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable auto-broadcast

print("Starting naive join...")
start_time = time.time()

# Perform join (will use SortMergeJoin - slow with skew)
df_naive = df_transactions_skewed.join(df_customers, "customer_id", "left")

# Force execution by counting
naive_count = df_naive.count()

naive_time = time.time() - start_time

print(f"\n✓ Naive join completed")
print(f"   Rows: {naive_count:,}")
print(f"   Time: {naive_time:.2f} seconds")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyze Naive Join Plan

# COMMAND ----------

# PURPOSE: Show the query plan - look for SortMergeJoin
# PROBLEM: SortMergeJoin + Skew = slow performance

print("\n📋 NAIVE JOIN EXECUTION PLAN")
print("="*60)
print("Looking for: SortMergeJoin (indicates shuffle-based join)")
print("Problem: Skewed data causes one task to process 400K rows")
print("="*60 + "\n")

df_naive.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC 📸 **SCREENSHOT #1: SPARK UI - NAIVE JOIN**
# MAGIC 
# MAGIC **How to capture:**
# MAGIC 1. Go to Spark UI (cluster page → Spark UI)
# MAGIC 2. Click on "Jobs" tab
# MAGIC 3. Find the most recent job (the count() operation)
# MAGIC 4. Click on the job → go to "Stages"
# MAGIC 5. Find the stage with the join operation
# MAGIC 6. Look at "Task Metrics" or "Event Timeline"
# MAGIC 
# MAGIC **What to look for:**
# MAGIC - One task takes MUCH longer than others
# MAGIC - Uneven task durations (some finish in seconds, one takes minutes)
# MAGIC - Skewed shuffle read sizes
# MAGIC 
# MAGIC **Save as:** `docs/screenshots/spark_ui_skew.png`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. OPTIMIZED JOIN (AQE + Broadcast - Fix the Problem)

# COMMAND ----------

# PURPOSE: Fix the performance issue with Spark optimizations
# APPROACH: Enable AQE and use broadcast join
# EXPECTATION: Much faster, balanced task execution

print("\n" + "🚀 OPTIMIZED JOIN (AQE + Broadcast)")
print("="*60)
print("Configuration:")
print("  - Adaptive Query Execution (AQE): ENABLED")
print("  - Skew Join Handling: ENABLED")
print("  - Broadcast Join: ENABLED (for small dimension)")
print("="*60 + "\n")

# Enable optimizations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB")

print("Starting optimized join...")
start_time = time.time()

# Explicitly broadcast the small dimension
# This copies the customer table to all executors (eliminates shuffle)
df_optimized = df_transactions_skewed.join(
    F.broadcast(df_customers),  # Broadcast hint
    "customer_id",
    "left"
)

# Force execution
optimized_count = df_optimized.count()

optimized_time = time.time() - start_time

print(f"\n✓ Optimized join completed")
print(f"   Rows: {optimized_count:,}")
print(f"   Time: {optimized_time:.2f} seconds")
print("="*60)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analyze Optimized Join Plan

# COMMAND ----------

# PURPOSE: Show the query plan - look for BroadcastHashJoin
# SOLUTION: BroadcastHashJoin = no shuffle, fast execution

print("\n📋 OPTIMIZED JOIN EXECUTION PLAN")
print("="*60)
print("Looking for: BroadcastHashJoin (indicates broadcast optimization)")
print("Solution: Small table copied to all nodes, no shuffle needed")
print("="*60 + "\n")

df_optimized.explain("formatted")

print("\n💡 KEY DIFFERENCE:")
print("   Naive:     SortMergeJoin (shuffle both sides)")
print("   Optimized: BroadcastHashJoin (broadcast small side, no shuffle)")

# COMMAND ----------

# MAGIC %md
# MAGIC 📸 **SCREENSHOT #2: SPARK UI - OPTIMIZED JOIN**
# MAGIC 
# MAGIC **Capture the same view as before:**
# MAGIC - Go to Spark UI → Jobs → Latest job → Stages
# MAGIC - Look at task durations
# MAGIC 
# MAGIC **What to look for:**
# MAGIC - All tasks finish in similar time (balanced)
# MAGIC - No skewed shuffle reads
# MAGIC - Faster overall execution
# MAGIC 
# MAGIC **Save as:** `docs/screenshots/spark_ui_fixed.png`

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Performance Comparison & Analysis

# COMMAND ----------

# Calculate speedup
if optimized_time > 0:
    speedup = naive_time / optimized_time
else:
    speedup = 0

print("\n" + "="*60)
print(" " * 15 + "PERFORMANCE COMPARISON")
print("="*60)
print(f"Naive join (no optimization):     {naive_time:>6.2f} seconds")
print(f"Optimized join (AQE + broadcast): {optimized_time:>6.2f} seconds")
print("-"*60)
print(f"Speedup:                          {speedup:>6.2f}x faster")
print(f"Time saved:                       {naive_time - optimized_time:>6.2f} seconds")
print("="*60)

# COMMAND ----------

# Verify both joins produced same results
print(f"\n✅ CORRECTNESS CHECK:")
print(f"   Naive result count:     {naive_count:,}")
print(f"   Optimized result count: {optimized_count:,}")
print(f"   Match: {naive_count == optimized_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Key Takeaways & Production Recommendations

# COMMAND ----------

print("""
╔══════════════════════════════════════════════════════════════╗
║           SPARK PERFORMANCE TUNING - SUMMARY                 ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║ PROBLEM REPRODUCED:                                          ║
║ • Data skew: 40% of data in one partition                    ║
║ • Hot key: customer_id = 1 had 400,000 transactions          ║
║ • Symptom: One task takes much longer than others            ║
║ • Impact: Poor cluster utilization, wasted resources         ║
║                                                              ║
║ NAIVE JOIN ISSUES:                                           ║
║ • SortMergeJoin with skewed partitions                       ║
║ • Shuffle both sides of join                                 ║
║ • One massive partition causes stragglers                    ║
║ • Total time dominated by slowest task                       ║
║                                                              ║
║ OPTIMIZATIONS APPLIED:                                       ║
║ • Broadcast join for small dimension (<10MB)                 ║
║ • Eliminates shuffle for small side                          ║
║ • AQE skew join handling for large-large joins               ║
║ • All executors process data in parallel                     ║
║                                                              ║
║ RESULTS:                                                     ║
║ • {speedup:.2f}x faster execution                                     ║
║ • Balanced task distribution                                 ║
║ • Better resource utilization                                ║
║ • Lower costs (faster = cheaper on cloud)                    ║
║                                                              ║
║ PRODUCTION RECOMMENDATIONS:                                  ║
║ ────────────────────────────────────────────────────────────  ║
║ 1. Monitor skew in Spark UI (Stages → Task Metrics)          ║
║ 2. Broadcast dimensions < 100MB if memory allows             ║
║ 3. Enable AQE for automatic skew handling                    ║
║ 4. Consider salting keys for extreme skew cases              ║
║ 5. Use repartition() to balance data before joins            ║
║ 6. Profile joins regularly as data grows                     ║
║                                                              ║
║ SKILLS DEMONSTRATED:                                         ║
║ • Diagnose skew in Spark UI                                  ║
║ • Understand join strategies (shuffle vs broadcast)          ║
║ • Configure AQE settings                                     ║
║ • Measure performance improvements                           ║
║ • Apply production-grade optimizations                       ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
""".format(speedup=speedup))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Deep Dive: Understanding Join Strategies

# COMMAND ----------

print("""
📚 SPARK JOIN STRATEGIES EXPLAINED

1. SORT MERGE JOIN (Default for large-large joins)
   ✓ When: Both tables are large
   ✓ How: Sort both sides, then merge
   ✗ Problem: Requires shuffle (expensive)
   ✗ Skew Impact: Very bad - one partition gets huge

2. BROADCAST HASH JOIN (Best for small-large joins)
   ✓ When: One table is small (<10MB default)
   ✓ How: Copy small table to all executors
   ✓ Benefit: No shuffle needed, very fast
   ✗ Limitation: Small table must fit in memory

3. SHUFFLE HASH JOIN
   ✓ When: Medium-sized tables
   ✓ How: Shuffle both, build hash table
   ✓ Better than: Sort merge for some cases

4. BROADCAST NESTED LOOP JOIN
   ✗ Usually: Slowest (Cartesian product)
   ✓ When: Non-equi joins, or very small tables

AQE (Adaptive Query Execution) can:
• Switch from sort merge to broadcast mid-query
• Split skewed partitions automatically
• Coalesce small partitions
• Optimize based on runtime statistics
""")

# COMMAND ----------

# Show configuration used
print("🔧 OPTIMIZATION SETTINGS USED:\n")
print(f"spark.sql.adaptive.enabled = {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"spark.sql.adaptive.skewJoin.enabled = {spark.conf.get('spark.sql.adaptive.skewJoin.enabled')}")
print(f"spark.sql.autoBroadcastJoinThreshold = {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary & Next Steps
# MAGIC 
# MAGIC **What we demonstrated:**
# MAGIC - ✓ Reproduced real-world data skew (40% in one partition)
# MAGIC - ✓ Measured performance impact (naive join timing)
# MAGIC - ✓ Fixed with broadcast join + AQE
# MAGIC - ✓ Compared execution plans (SortMerge vs BroadcastHash)
# MAGIC - ✓ Documented Spark UI evidence (screenshots)
# MAGIC 
# MAGIC **Screenshots required:**
# MAGIC 1. `docs/screenshots/spark_ui_skew.png` - Naive join showing task imbalance
# MAGIC 2. `docs/screenshots/spark_ui_fixed.png` - Optimized join showing balanced execution
# MAGIC 
# MAGIC **Business value:**
# MAGIC - Faster queries → happier users
# MAGIC - Better resource utilization → lower costs
# MAGIC - Scalable patterns → handles data growth
# MAGIC - Production-ready → can handle real-world skew

