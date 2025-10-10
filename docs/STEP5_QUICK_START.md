# Step 5: Performance Tuning (Skew vs Broadcast) - Quick Start Guide

## What Was Implemented

### File Created
**`notebooks/11_perf_skew_broadcast.py`** - Spark performance optimization demo

### Performance Patterns Demonstrated
1. **Data skew reproduction** - 40% of data in one partition
2. **Naive join** - SortMergeJoin with skew (slow)
3. **Optimized join** - BroadcastHashJoin (fast)
4. **Spark UI analysis** - Task distribution comparison
5. **AQE configuration** - Adaptive Query Execution tuning

## Prerequisites

**No previous steps required** - This is a standalone performance demo using synthetic data

## How to Run on Databricks

### Step-by-Step Execution

#### 1. Open the Notebook
- Navigate to `notebooks/11_perf_skew_broadcast.py`
- Attach to your cluster (same as previous steps)

#### 2. Run All Cells
Execute cells in order (or "Run All"):

**Section 1: Create Skewed Data (cells 1-6)**
- Generate 1M transactions with 40% skew
- Verify skew with GROUP BY analysis

**Section 2: Create Customer Dimension (cell 7)**
- Small dimension (1,000 customers, ~10KB)

**Section 3: Naive Join (cells 8-10)**
- Disable AQE and broadcast
- Run slow join with SortMergeJoin
- **📸 Capture Spark UI screenshot #1**

**Section 4: Optimized Join (cells 11-13)**
- Enable AQE and broadcast
- Run fast join with BroadcastHashJoin
- **📸 Capture Spark UI screenshot #2**

**Section 5: Comparison (cells 14-18)**
- Performance metrics
- Key takeaways
- Production recommendations

### Expected Output

#### Section 1 (Skewed Data):
```
🔧 GENERATING SKEWED DATASET
============================================================
Simulating: 1 million transactions with heavy skew
Problem: Customer ID 1 has 40% of all transactions (hot key)

✓ Normal data: 600,000 rows across 1,000 customers
✓ Skewed data: 400,000 rows for customer_id = 1

📊 FINAL DATASET:
   Total transactions: 1,000,000
   Skew level: 40% in one customer_id

📊 SKEW METRICS:
   Customer 1 transactions: 400,000
   Percentage of total: 40.0%
   Problem: One partition will have 40% of the data!
```

#### Section 3 (Naive Join):
```
🐌 NAIVE JOIN (No Optimizations)
Configuration:
  - Adaptive Query Execution (AQE): DISABLED
  - Broadcast Join: DISABLED

✓ Naive join completed
   Rows: 1,000,000
   Time: XX.XX seconds
```

#### Section 4 (Optimized Join):
```
🚀 OPTIMIZED JOIN (AQE + Broadcast)
Configuration:
  - Adaptive Query Execution (AQE): ENABLED
  - Broadcast Join: ENABLED

✓ Optimized join completed
   Rows: 1,000,000
   Time: X.XX seconds
```

#### Section 5 (Comparison):
```
============================================================
               PERFORMANCE COMPARISON
============================================================
Naive join (no optimization):     XX.XX seconds
Optimized join (AQE + broadcast):  X.XX seconds
------------------------------------------------------------
Speedup:                           X.XXx faster
Time saved:                        XX.XX seconds
============================================================
```

## What This Proves (Interview Skills)

| Skill | Evidence |
|-------|----------|
| **Skew diagnosis** | Created and identified skewed data distribution |
| **Join strategies** | Compared SortMergeJoin vs BroadcastHashJoin |
| **Spark UI analysis** | Read task metrics, identified stragglers |
| **AQE configuration** | Set skew thresholds, broadcast limits |
| **Performance tuning** | Measured 2-10x speedup with optimizations |
| **Production knowledge** | Documented best practices and recommendations |

## Screenshots Required (Critical!)

### Screenshot #1: Naive Join (Skew Problem)
**File:** `docs/screenshots/spark_ui_skew.png`

**How to capture:**
1. After running the naive join (Section 3)
2. Go to Databricks cluster page → **Spark UI**
3. Click **Jobs** tab
4. Find the most recent job (the `count()` operation)
5. Click on that job → **Stages** tab
6. Find the stage with the join operation
7. Look at **Event Timeline** or **Task Metrics**

**What to capture:**
- Screenshot showing **task duration imbalance**
- One task takes MUCH longer than others
- Uneven bars in the timeline
- Skewed shuffle read sizes

### Screenshot #2: Optimized Join (Fixed)
**File:** `docs/screenshots/spark_ui_fixed.png`

**How to capture:**
1. After running the optimized join (Section 4)
2. Same steps as above
3. Find the latest job with the optimized join

**What to capture:**
- Screenshot showing **balanced task execution**
- All tasks finish in similar time
- Even bars in the timeline
- No shuffle (or minimal shuffle)

## Key Concepts Explained

### Data Skew
```
Customer 1:  ████████████████████████████████████████ (400K rows - 40%)
Customer 2:  ██ (600 rows)
Customer 3:  ██ (600 rows)
...
Customer 1000: ██ (600 rows)
```
**Problem:** One Spark task processes 400K rows while others process 600

### Join Strategies

**SortMergeJoin (Naive):**
```
Transactions (1M) ──shuffle──┐
                              ├─ Sort & Merge
Customers (1K)    ──shuffle──┘
Problem: Both sides shuffle, skew causes stragglers
```

**BroadcastHashJoin (Optimized):**
```
Transactions (1M) ──no shuffle──┐
                                 ├─ Hash Join
Customers (1K)    ──broadcast───→ (copied to all nodes)
Solution: No shuffle, small table in memory everywhere
```

### AQE (Adaptive Query Execution)
```
Benefits:
• Runtime statistics → better decisions
• Can switch join strategies mid-query
• Splits skewed partitions automatically
• Coalesces small partitions
```

## Validation Tests

### Test 1: Verify Skew Exists
```python
skew_check = df.groupBy("customer_id").count().orderBy(F.desc("count"))
top_customer = skew_check.first()
print(f"Top customer has {top_customer['count']:,} rows")
# Expected: 400,000 (40% of 1M)
```

### Test 2: Check Query Plans
```python
# Naive should show SortMergeJoin
df_naive.explain("formatted")
# Look for: "SortMergeJoin"

# Optimized should show BroadcastHashJoin
df_optimized.explain("formatted")
# Look for: "BroadcastHashJoin"
```

### Test 3: Performance Improvement
```
Speedup should be:
• 2-5x faster (minimum)
• 5-10x faster (typical)
• 10-20x faster (with good cluster)
```

## Troubleshooting

### Issue: Both joins show similar performance
**Cause:** Cluster too small, or broadcast auto-enabled on naive join  
**Solution:** Check that `spark.sql.autoBroadcastJoinThreshold = -1` for naive join

### Issue: Can't find Spark UI
**Cause:** Cluster might be terminated  
**Solution:** Spark UI only available while cluster is running. Keep cluster alive while capturing screenshots.

### Issue: No visible skew in UI
**Cause:** Dataset too small, or cluster auto-optimizing  
**Solution:** Increase dataset size (change 1M to 10M rows)

### Issue: Broadcast join fails
**Cause:** Dimension table too large for broadcast  
**Solution:** This is a demo with small dim (1K rows). In production, only broadcast tables <100MB.

## Production Best Practices

### 1. Diagnose Skew
```python
# Check data distribution
df.groupBy("join_key").count().orderBy(F.desc("count")).show(10)

# Look for:
# - One key with 10x+ more rows than others
# - Partition size >256MB in Spark UI
```

### 2. Fix Skew Options

**Option A: Broadcast (for small tables)**
```python
df_large.join(F.broadcast(df_small), "key")
```

**Option B: Salting (for large-large joins)**
```python
# Add random salt to distribute hot keys
df_large.withColumn("salt", F.floor(F.rand() * 10))
```

**Option C: AQE (automatic)**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### 3. Monitor Performance
```
Regular checks:
• Spark UI → Stages → Task duration distribution
• Look for tasks taking 10x longer than median
• Check shuffle read sizes
• Monitor executor idle time
```

## Next Steps

**Step 5 Complete!** ✅

Remaining optional steps:
- **Step 6:** Testing with pytest + chispa (data quality tests)
- **Step 7:** BI integration (dashboard, Power BI refresh)

See `docs/IMPLEMENTATION_PLAN.md` for details

## Exit Criteria Checklist

- [ ] Notebook runs without errors
- [ ] Skewed dataset created (40% in one key)
- [ ] Naive join executed and timed
- [ ] Optimized join executed and timed
- [ ] Speedup measured (2x+ improvement)
- [ ] Query plans show SortMerge vs BroadcastHash
- [ ] Screenshot #1 captured (skew problem)
- [ ] Screenshot #2 captured (optimized solution)
- [ ] Performance comparison documented

**When all boxes checked, Step 5 is complete!** 🎉

## Business Value Summary

**Performance:** 2-10x faster queries  
**Cost:** Faster execution = lower compute costs on cloud  
**Scalability:** Patterns work as data grows  
**Reliability:** Prevents out-of-memory errors from skew  
**Skills:** Production-ready optimization knowledge

