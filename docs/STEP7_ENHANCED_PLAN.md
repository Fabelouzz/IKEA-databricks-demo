# Step 7 Enhanced Plan: Visualizing Engineering Concepts

## Philosophy Shift

Instead of generic BI dashboards (revenue charts, KPIs), we'll **visualize the data engineering concepts** to prove they work:
- Delta Time Travel in action
- Performance tuning results
- Metadata-driven architecture
- Data quality gates
- API ingestion patterns

## Enhancements to Previous Steps

### Enhancement 1: Expand Corrupted Data (Step 4)

**Current**: 1 bad row inserted and deleted  
**Enhanced**: Multiple corruption scenarios to showcase different governance patterns

**New corrupted data scenarios**:
1. **Orphan foreign keys** (5 rows): customer_id doesn't exist in dimension
2. **Duplicate primary keys** (3 rows): Same receipt_id inserted twice
3. **Out-of-range dates** (2 rows): Dates from 1900 or 2099
4. **Null violations** (4 rows): Null values in required fields
5. **Data type corruption** (3 rows): Negative amounts, impossible values

**Total**: ~15-20 corrupted rows across different violation types

**Why**: Shows DELETE with complex WHERE clauses, history tracking for multiple operations, and real-world data quality scenarios.

### Enhancement 2: Extreme Performance Difference (Step 5)

**Current**: 0.6s vs 4s (~6.7x speedup)  
**Enhanced**: 2s vs 25s+ (~12x+ speedup)

**How to achieve**:
- Increase dataset size: 600k normal + 400k skewed → **6M normal + 4M skewed**
- Increase customer dimension: 1,000 → **10,000 customers**
- Force multiple shuffle stages with aggregations before join
- Disable Spark optimizations more explicitly for naive run
- Add `repartition(1)` on skewed side to force single task bottleneck

**Spark UI clarity**:
- Add intermediate `.cache()` and `.count()` to create distinct jobs
- Name DataFrames with `.alias()` for readable stage names
- Add custom Spark SQL query names with `spark.conf.set` descriptions

**Why**: More dramatic demonstration, easier to see stragglers in Spark UI stages.

---

## Step 7: Visual Implementation Plan

### Artifact 1: Databricks SQL Notebook - Engineering Concepts Dashboard
**File**: `notebooks/12_bi_concepts_demo.sql`

**Section 1: Delta Time Travel Explorer**
- Query 1: Full DESCRIBE HISTORY with operation metrics
- Query 2: Version comparison (before/after delete counts)
- Query 3: Corrupted rows by violation type (from deleted versions)
- Query 4: Time-based recovery window (versions in last 7 days)

**Section 2: Performance Tuning Evidence**
- Query 1: Performance runs table (naive vs optimized times)
- Query 2: Speedup calculation and trending
- Query 3: Spark config differences table
- Query 4: Dataset size metrics (skew factor, partition count)

**Section 3: Metadata-Driven Joins Proof**
- Query 1: YAML config inspection (parsed and displayed)
- Query 2: Join lineage (base → joins → output)
- Query 3: Broadcast hint effectiveness (output table stats)
- Query 4: Column provenance (which columns from which source)

**Section 4: Data Quality Gates Monitor**
- Query 1: Composite key uniqueness checks (should be 0 violations)
- Query 2: Rate bounds validation (should be 0 out-of-bounds)
- Query 3: Null PK detection (should be 0 nulls)
- Query 4: Orphan foreign key detection (anti-joins)
- Query 5: Date range validation

**Section 5: API Ingestion Operations**
- Query 1: Bronze landed counts vs API totals (pagination proof)
- Query 2: FX coverage calendar (daily completeness)
- Query 3: Ingestion freshness (latest ingested_at)
- Query 4: Source breakdown (DummyJSON vs Frankfurter)

**Section 6: CTE Transform Validation**
- Query 1: Deduplication effectiveness (duplicate count should be 0)
- Query 2: Size class distribution (LARGE vs SMALL)
- Query 3: Name concatenation samples
- Query 4: Schema evolution tracking

### Artifact 2: Performance Tracking Table
**File**: `notebooks/11_perf_skew_broadcast.py` (enhanced)

**New addition**: Write performance metrics to `ops.perf_runs` table:
```python
perf_data = spark.createDataFrame([
    (
        datetime.now(),
        "skew_join_demo_v2",
        naive_time,
        optimized_time,
        naive_time / optimized_time,
        df_transactions_skewed.count(),
        df_customers.count(),
        "AQE disabled, no broadcast",
        "AQE enabled, broadcast hint",
        "SortMergeJoin",
        "BroadcastHashJoin"
    )
], [
    "run_timestamp", "test_name", "naive_time_s", "optimized_time_s", 
    "speedup_factor", "fact_rows", "dim_rows", "naive_config", 
    "optimized_config", "naive_plan", "optimized_plan"
])

perf_data.write.mode("append").saveAsTable("ops.perf_runs")
```

### Artifact 3: Corrupted Data Catalog
**File**: `notebooks/10_delta_time_travel.py` (enhanced)

**New addition**: Track corruption types before deletion:
```python
corruption_catalog = spark.createDataFrame([
    (receipt_id, violation_type, severity, detected_at, deleted_at, version_deleted)
    for each corrupted row
], schema)

corruption_catalog.write.mode("append").saveAsTable("ops.data_quality_violations")
```

### Artifact 4: Databricks Lakeview Dashboard
**File**: `docs/LAKEVIEW_BUILD_GUIDE.md`

**Dashboard Layout** (6 tabs):

**Tab 1: Delta Governance**
- Tile 1 (table): DESCRIBE HISTORY (last 20 operations)
- Tile 2 (KPI): Total versions available
- Tile 3 (bar chart): Operations by type (CREATE, DELETE, MERGE, UPDATE)
- Tile 4 (table): Corrupted rows by violation type (from ops.data_quality_violations)
- Tile 5 (line chart): Table size over time (operationMetrics.numOutputRows)
- Tile 6 (text): "How to read: Each operation creates a new version. DELETE operations are auditable."

**Tab 2: Performance Tuning**
- Tile 1 (KPI): Latest speedup factor (e.g., "12.5x faster")
- Tile 2 (bar chart): Naive vs Optimized time (side-by-side)
- Tile 3 (line chart): Speedup trend over multiple runs
- Tile 4 (table): Spark config comparison
- Tile 5 (image): Spark UI screenshot - skewed
- Tile 6 (image): Spark UI screenshot - fixed
- Tile 7 (text): "How to read: Broadcast join eliminates shuffle, AQE handles remaining skew."

**Tab 3: Metadata Architecture**
- Tile 1 (HTML/iframe): Mermaid diagram of join flow (from YAML)
- Tile 2 (table): Join config details (table, alias, type, on clause)
- Tile 3 (KPI): Number of joins defined in YAML
- Tile 4 (table): Output table column provenance
- Tile 5 (text): "How to read: All joins are config-driven. Add new tables via YAML, not code."

**Tab 4: Data Quality Monitor**
- Tile 1 (KPI): Composite key violations (should be 0)
- Tile 2 (KPI): Rate bound violations (should be 0)
- Tile 3 (KPI): Null PK violations (should be 0)
- Tile 4 (KPI): Orphan FK count (should be 0)
- Tile 5 (table): Quality check details
- Tile 6 (conditional formatting): All KPIs green if 0, red if > 0
- Tile 7 (text): "How to read: These metrics mirror the pytest test suite. All should be 0."

**Tab 5: API Ingestion**
- Tile 1 (KPI): DummyJSON products landed (vs API total)
- Tile 2 (KPI): Frankfurter FX pairs count
- Tile 3 (calendar heatmap): FX coverage by date
- Tile 4 (bar chart): Ingestion counts over time
- Tile 5 (KPI): Latest ingestion timestamp
- Tile 6 (image): Postman API validation screenshot
- Tile 7 (text): "How to read: Pagination fetched all records. FX coverage shows weekday-only data."

**Tab 6: Transform Validation**
- Tile 1 (KPI): Products deduplicated (before vs after)
- Tile 2 (pie chart): Size class distribution (LARGE vs SMALL)
- Tile 3 (table): Sample transformed records
- Tile 4 (bar chart): Records per transformation stage (bronze → silver → gold)
- Tile 5 (text): "How to read: CTEs cleaned and deduplicated raw data. All PKs are unique."

### Artifact 5: Mermaid Diagram Generator
**File**: `notebooks/13_generate_join_diagram.py`

Reads `config/joins.yml` and generates:
1. Mermaid diagram code
2. HTML with embedded Mermaid
3. SVG export for documentation

### Artifact 6: Power BI Stub (Optional)
**File**: `ops/powerbi_refresh.py` (already in plan, keep as-is)

---

## Implementation Order

1. ✅ Enhance `10_delta_time_travel.py` - Add 15-20 corrupted rows across 5 violation types
2. ✅ Enhance `11_perf_skew_broadcast.py` - Scale to 10M rows, write metrics to `ops.perf_runs`
3. ✅ Create `12_bi_concepts_demo.sql` - All dashboard queries organized by section
4. ✅ Create `13_generate_join_diagram.py` - Mermaid diagram from YAML
5. ✅ Create `docs/LAKEVIEW_BUILD_GUIDE.md` - Step-by-step dashboard assembly
6. ✅ Create `docs/STEP7_QUICK_START.md` - How to run and validate
7. ✅ Create `docs/STEP7_SUMMARY.md` - What was built and why

---

## Expected Outputs

**Screenshots to capture**:
1. `docs/screenshots/delta_history_expanded.png` - History with multiple DELETE operations
2. `docs/screenshots/spark_ui_skew_extreme.png` - 1 task taking 20s while others idle
3. `docs/screenshots/spark_ui_fixed_extreme.png` - All tasks balanced, ~2s
4. `docs/screenshots/lakeview_delta_tab.png` - Delta governance dashboard
5. `docs/screenshots/lakeview_perf_tab.png` - Performance tuning dashboard
6. `docs/screenshots/lakeview_metadata_tab.png` - Metadata architecture diagram
7. `docs/screenshots/lakeview_dq_tab.png` - Data quality monitor (all green)
8. `docs/screenshots/join_diagram_mermaid.png` - Generated join flow diagram

**Tables created**:
- `ops.perf_runs` - Performance test results over time
- `ops.data_quality_violations` - Catalog of detected corruption (deleted rows)

**Queries created**:
- 30+ SQL queries in `12_bi_concepts_demo.sql` organized by concept

---

## Time Budget

- Enhance Step 4 (corrupted data): 30 min
- Enhance Step 5 (extreme perf): 45 min
- Create `12_bi_concepts_demo.sql`: 1.5 hours
- Create `13_generate_join_diagram.py`: 30 min
- Create Lakeview build guide: 1 hour
- Documentation (Quick Start, Summary): 1 hour
- **Total**: ~5 hours

---

## Why This Is Better Than Generic BI

**Generic BI approach** (revenue, sales, KPIs):
- ❌ Doesn't showcase data engineering skills
- ❌ Looks like every other analytics dashboard
- ❌ Interviewer thinks: "Nice charts, but did you build the pipeline?"

**Engineering concepts approach** (this plan):
- ✅ Proves you understand Delta internals (history, time travel)
- ✅ Shows performance tuning with measurable results
- ✅ Demonstrates architecture thinking (metadata-driven)
- ✅ Validates data quality systematically
- ✅ Interviewer thinks: "This person knows how systems work, not just SQL."

---

## Next: Start Implementation

I'll now implement all enhancements in order. Ready to proceed?

