# Databricks Lakeview Dashboard Build Guide

## Overview

This guide explains how to build a **multi-tab engineering concepts dashboard** in Databricks Lakeview that visualizes:
- Delta governance (time travel, history)
- Performance tuning results
- Metadata-driven architecture  
- Data quality gates
- API ingestion patterns
- Transform validations

**Why not generic BI?** This dashboard proves you understand *how systems work*, not just how to write SQL.

---

## Prerequisites

1. Completed Steps 1-6 of the implementation plan
2. Access to Databricks SQL workspace
3. Permissions to create dashboards
4. All queries from `notebooks/12_bi_concepts_demo.sql` executed successfully

---

## Dashboard Structure

**6 Tabs**, ~30 tiles total:

1. **Delta Governance** - Time travel, history, violations
2. **Performance Tuning** - Speedup metrics, Spark UI evidence
3. **Metadata Architecture** - Join diagram, config details
4. **Data Quality Monitor** - Quality gates, all should be 0/green
5. **API Ingestion** - Bronze stats, FX coverage, freshness
6. **Transform Validation** - Deduplication, size class, funnel

---

## Step-by-Step Build Instructions

### 1. Create New Lakeview Dashboard

1. Open Databricks SQL workspace
2. Click **Dashboards** in left sidebar
3. Click **Create Dashboard**
4. Name: `Engineering Concepts Dashboard`
5. Description: `Visualization of data engineering patterns: Delta governance, performance tuning, metadata architecture, data quality`

---

### 2. TAB 1: Delta Governance

**Purpose**: Prove Delta Lake governance features work (DELETE, history, time travel)

#### Tile 1.1: Full History Table
- **Type**: Table
- **Query**: Section 1, Query 1.1 from `12_bi_concepts_demo.sql`
- **Title**: `Delta Table History (gold.baskets_enriched)`
- **Description**: `Complete audit trail of all operations`
- **Columns to show**: version, timestamp, operation, operationParameters, operationMetrics
- **Format**: 
  - Timestamp: `YYYY-MM-DD HH:mm:ss`
  - operationMetrics: Expand JSON (numOutputRows, numRemovedFiles)
- **Sort**: version DESC
- **Limit**: 50 rows

#### Tile 1.2: Operations by Type (Bar Chart)
- **Type**: Bar chart
- **Query**: Section 1, Query 1.2
- **Title**: `Operations by Type`
- **X-axis**: operation
- **Y-axis**: operation_count
- **Color**: Blue gradient
- **Sort**: Count descending

#### Tile 1.3: Table Size Over Time (Line Chart)
- **Type**: Line chart
- **Query**: Section 1, Query 1.3
- **Title**: `Table Size Evolution`
- **X-axis**: version
- **Y-axis**: output_rows
- **Line color**: Green
- **Markers**: Show points
- **Tooltip**: Include operation, timestamp

#### Tile 1.4: Violations by Type (Table with Conditional Formatting)
- **Type**: Table
- **Query**: Section 1, Query 1.4
- **Title**: `Data Quality Violations Detected`
- **Columns**: violation_type, severity, violation_count, examples
- **Conditional Formatting**:
  - severity = 'CRITICAL': Red background
  - severity = 'HIGH': Orange background
  - severity = 'MEDIUM': Yellow background
- **Sort**: severity (custom order: CRITICAL, HIGH, MEDIUM)

#### Tile 1.5: KPI - Total Violations
- **Type**: Counter (KPI)
- **Query**: `SELECT COUNT(*) AS total_violations FROM ops.data_quality_violations`
- **Title**: `Total Violations Detected`
- **Value**: total_violations
- **Format**: Number with commas
- **Color**: Red (alert color)

#### Tile 1.6: Text Widget - How to Read
- **Type**: Text/Markdown
- **Content**:
  ```markdown
  ## 📖 How to Read This Tab
  
  **Purpose**: Demonstrate Delta Lake governance capabilities
  
  **What you're seeing**:
  - Full history of table operations (CREATE, WRITE, DELETE, MERGE)
  - Data quality violations detected before deletion
  - Table size changes across versions
  
  **Interview talking point**: 
  "Delta Lake provides a complete audit trail. We can track every operation, time-travel to any version, and prove data corrections were applied. The violations table shows 17 corrupted rows across 5 scenarios that were detected and removed using surgical DELETE operations."
  ```

---

### 3. TAB 2: Performance Tuning

**Purpose**: Show measurable performance improvements from broadcast joins and AQE

#### Tile 2.1: KPI - Latest Speedup
- **Type**: Counter (KPI)
- **Query**: Section 2, Query 2.1 (extract speedup_x)
- **Title**: `Speedup Factor`
- **Value**: speedup_x
- **Suffix**: `x faster`
- **Format**: 1 decimal place
- **Color**: Green
- **Font size**: Extra large

#### Tile 2.2: Naive vs Optimized Time (Bar Chart)
- **Type**: Grouped bar chart
- **Query**: Section 2, Query 2.2
- **Title**: `Naive vs Optimized Execution Time`
- **X-axis**: test_name
- **Y-axis**: Time (seconds)
- **Series**: 
  - naive_time_s (Red)
  - optimized_time_s (Green)
- **Legend**: Show
- **Labels**: Show values on bars

#### Tile 2.3: Speedup Trend (Line Chart)
- **Type**: Line chart
- **Query**: Section 2, Query 2.3
- **Title**: `Performance Improvement Over Time`
- **X-axis**: run_timestamp
- **Y-axis**: speedup_factor
- **Line color**: Blue
- **Reference line**: Y = 1 (no improvement baseline)

#### Tile 2.4: Config Comparison (Table)
- **Type**: Table
- **Query**: Section 2, Query 2.4
- **Title**: `Configuration Comparison`
- **Columns**: scenario, configuration, join_type, execution_time_s
- **Row colors**:
  - Naive Config: Light red background
  - Optimized Config: Light green background

#### Tile 2.5: Spark UI Screenshot - Skewed
- **Type**: Image
- **Source**: `docs/screenshots/spark_ui_skew_extreme.png`
- **Title**: `Spark UI: Naive Join (Skewed)`
- **Caption**: `One task takes 25s (straggler), others idle`

#### Tile 2.6: Spark UI Screenshot - Fixed
- **Type**: Image
- **Source**: `docs/screenshots/spark_ui_fixed_extreme.png`
- **Title**: `Spark UI: Optimized Join (Balanced)`
- **Caption**: `All tasks ~2-3s, broadcast eliminates shuffle`

#### Tile 2.7: Text Widget - Analysis
- **Type**: Text/Markdown
- **Content**:
  ```markdown
  ## 🚀 Performance Analysis
  
  **Problem**: 40% of 10M rows in one key → severe data skew
  
  **Solution**:
  - Broadcast join (small dimension)
  - AQE skew handling
  - SortMergeJoin → BroadcastHashJoin
  
  **Result**: 12.5x speedup (2s vs 25s)
  
  **Cost savings**: ~$0.015/run (extrapolate to daily pipelines)
  
  **Interview talking point**: "I identified skew in Spark UI task metrics, diagnosed SortMergeJoin as the bottleneck, and fixed it with explicit broadcast hints. The result was a 12x speedup with zero code changes to business logic—just optimization config."
  ```

---

### 4. TAB 3: Metadata Architecture

**Purpose**: Visualize metadata-driven join configuration

#### Tile 3.1: Join Diagram (HTML iFrame)
- **Type**: HTML
- **Source**: Upload `docs/join_diagram.html` or embed content
- **Title**: `Join Lineage (Auto-Generated from YAML)`
- **Height**: 600px
- **Full width**: Yes

#### Tile 3.2: Join Config Details (Table)
- **Type**: Table
- **Query**: Section 3, Query 3.2 (column provenance)
- **Title**: `Column Provenance`
- **Columns**: column_name, source
- **Group by**: source
- **Collapsible rows**: Yes

#### Tile 3.3: Output Stats (KPI Grid)
- **Type**: Multiple counters
- **Query**: Section 3, Query 3.1
- **Tiles**:
  - Total Rows
  - Unique Receipts
  - Unique Customers
  - Rows with FX Data
- **Layout**: 4 columns

#### Tile 3.4: Broadcast Effectiveness (Stacked Bar)
- **Type**: Stacked bar chart
- **Query**: Section 3, Query 3.3
- **Title**: `Join Match Rates`
- **X-axis**: customer_join_status
- **Y-axis**: row_count
- **Stack by**: fx_join_status
- **Colors**: Green (matched), Orange (not matched)

#### Tile 3.5: Text Widget - Architecture
- **Type**: Text/Markdown
- **Content**:
  ```markdown
  ## 🏗️ Metadata-Driven Architecture
  
  **Pattern**: All joins defined in `config/joins.yml`, not code
  
  **Scalability**: Add 10-20 tables via YAML edits, zero Python changes
  
  **Self-documenting**: Diagram auto-generated from config
  
  **Interview talking point**: "Instead of hardcoding joins in Spark scripts, I use a YAML config that defines tables, aliases, join types, and broadcast hints. A generic join engine reads the config and applies joins dynamically. This proves I think about maintainability and scalability, not just getting queries to run."
  ```

---

### 5. TAB 4: Data Quality Monitor

**Purpose**: Show data quality gates catch issues (all metrics should be 0)

#### Tile 4.1: Quality Gate Summary (Table with Conditional Formatting)
- **Type**: Table
- **Query**: Section 4, Query 4.6
- **Title**: `Quality Gate Summary`
- **Columns**: quality_check, violation_count, expected
- **Conditional Formatting**:
  - violation_count = 0: Green background, ✅ prefix
  - violation_count > 0: Red background, ❌ prefix
- **Sort**: violation_count DESC

#### Tile 4.2-4.6: Individual Quality KPIs
Create 5 separate counter tiles:

**Tile 4.2**: Composite Key Duplicates
- **Query**: Section 4, Query 4.1
- **Value**: duplicate_composite_keys
- **Expected**: 0
- **Color**: Green if 0, Red if > 0

**Tile 4.3**: Invalid Rates
- **Query**: Section 4, Query 4.2
- **Value**: invalid_rates
- **Expected**: 0

**Tile 4.4**: Null PKs
- **Query**: Section 4, Query 4.3
- **Value**: null_product_ids
- **Expected**: 0

**Tile 4.5**: Orphan FKs
- **Query**: Section 4, Query 4.4
- **Value**: orphan_customer_ids
- **Expected**: 0

**Tile 4.6**: Out-of-Range Dates
- **Query**: Section 4, Query 4.5
- **Value**: out_of_range_dates
- **Expected**: 0

**Layout**: 2 rows × 3 columns grid

#### Tile 4.7: Text Widget - Quality Philosophy
- **Type**: Text/Markdown
- **Content**:
  ```markdown
  ## ✅ Data Quality Philosophy
  
  **These metrics mirror the pytest test suite** (`tests/test_transforms.py`)
  
  **All should be 0** (green):
  - Composite key duplicates → Deduplication works
  - Invalid rates → Bounds checks work
  - Null PKs → Schema enforcement works
  - Orphan FKs → Referential integrity maintained
  - Bad dates → Range validation works
  
  **Interview talking point**: "I don't just run transforms and hope they're correct. I implement quality gates that mirror my unit tests. These dashboards track the same metrics as my pytest suite, proving that data quality is validated both in CI/CD and in production monitoring."
  ```

---

### 6. TAB 5: API Ingestion

**Purpose**: Prove pagination, freshness, and completeness

#### Tile 5.1: Bronze Counts (Bar Chart)
- **Type**: Bar chart
- **Query**: Section 5, Query 5.1
- **Title**: `Rows Landed by Source`
- **X-axis**: source
- **Y-axis**: row_count
- **Color**: Blue
- **Labels**: Show counts

#### Tile 5.2: FX Coverage Calendar (Heatmap or Line Chart)
- **Type**: Line chart (if heatmap not available)
- **Query**: Section 5, Query 5.2
- **Title**: `FX Data Coverage (Last 90 Days)`
- **X-axis**: as_of_date
- **Y-axis**: pair_count
- **Tooltip**: Show pairs_available

#### Tile 5.3: Ingestion Freshness (Table)
- **Type**: Table
- **Query**: Section 5, Query 5.3
- **Title**: `Ingestion Freshness`
- **Columns**: source, latest_ingestion, days_since_ingest
- **Conditional Formatting**:
  - days_since_ingest < 1: Green
  - days_since_ingest 1-7: Yellow
  - days_since_ingest > 7: Red

#### Tile 5.4: Pagination Completeness (Table with %)
- **Type**: Table
- **Query**: Section 5, Query 5.4
- **Title**: `Pagination Completeness`
- **Columns**: entity, bronze_count, api_total, completion_pct
- **Format**: completion_pct as percentage with 1 decimal
- **Conditional Formatting**:
  - completion_pct ≥ 95: Green
  - completion_pct < 95: Orange

#### Tile 5.5: API Validation Screenshot
- **Type**: Image
- **Source**: `docs/screenshots/api_validation.png`
- **Title**: `Postman API Validation`
- **Caption**: `Verified endpoints before implementation`

#### Tile 5.6: Text Widget - Ingestion Patterns
- **Type**: Text/Markdown
- **Content**:
  ```markdown
  ## 📡 API Ingestion Patterns
  
  **Sources**:
  - DummyJSON: Products (194), Users (208)
  - Frankfurter: FX rates (~128 weekday dates)
  
  **Techniques**:
  - Pagination (skip/limit)
  - Retry logic (3 attempts, 2s wait)
  - Schema-on-write (explicit types)
  - Idempotent loads (overwrite mode)
  
  **Interview talking point**: "I validated APIs in Postman first, then implemented pagination with retry logic and explicit schemas. The dashboard proves we fetched all records (100% completeness) and tracks ingestion freshness. In production, this would trigger alerts if data goes stale."
  ```

---

### 7. TAB 6: Transform Validation

**Purpose**: Show CTEs cleaned data correctly

#### Tile 6.1: Deduplication Check (KPI)
- **Type**: Counter
- **Query**: Section 6, Query 6.1
- **Title**: `Duplicate Product IDs`
- **Value**: duplicate_product_ids
- **Expected**: 0
- **Color**: Green if 0

#### Tile 6.2: Size Class Distribution (Pie Chart)
- **Type**: Pie chart
- **Query**: Section 6, Query 6.2
- **Title**: `Product Size Class Distribution`
- **Slices**: size_class
- **Values**: product_count
- **Labels**: Show percentages
- **Colors**: Blue (SMALL), Orange (LARGE)

#### Tile 6.3: Pipeline Funnel (Funnel Chart or Bar)
- **Type**: Bar chart (horizontal)
- **Query**: Section 6, Query 6.3
- **Title**: `Transformation Pipeline Funnel`
- **X-axis**: row_count
- **Y-axis**: stage
- **Sort**: stage_order
- **Color gradient**: Bronze → Silver → Gold

#### Tile 6.4: Sample Transformed Records (Table)
- **Type**: Table
- **Query**: Section 6, Query 6.4
- **Title**: `Sample Enriched Records`
- **Columns**: All (receipt_id through attached)
- **Limit**: 20
- **Scrollable**: Yes

#### Tile 6.5: Text Widget - Transform Summary
- **Type**: Text/Markdown
- **Content**:
  ```markdown
  ## 🔄 Transform Validation
  
  **CTE patterns used**:
  - Clean → Dedup → Latest (ROW_NUMBER window function)
  - Primary/composite key enforcement
  - Size class mapping (CASE WHEN price > 1000)
  - Null filtering
  
  **Validation**:
  - ✅ No duplicate product_id
  - ✅ Size class correctly mapped
  - ✅ Pipeline funnel shows row counts at each layer
  
  **Interview talking point**: "I use CTEs to structure complex transforms in a readable, testable way. Each CTE has a clear purpose (clean, dedup, latest), and the dashboard validates that deduplication worked and business logic (size class) was applied correctly."
  ```

---

## 8. Dashboard-Level Configuration

### Filters (Global Parameters)
Add global filters that apply to all tabs:

1. **Date Range Filter**
   - Parameter: `date_range`
   - Type: Date range picker
   - Default: Last 90 days
   - Apply to queries with `date` or `timestamp` columns

2. **Table Version Filter** (for Delta history)
   - Parameter: `version_number`
   - Type: Number input
   - Default: Latest
   - Apply to time travel queries

### Refresh Schedule
- **Frequency**: Daily at 6 AM
- **Timezone**: UTC
- **Email on failure**: Yes (send to team)

### Permissions
- **Viewers**: All data team members
- **Editors**: DE team leads
- **Run as**: Service principal with read access to all schemas

---

## 9. Final Polish

### Dashboard Description
Add this to dashboard description:

```
This dashboard visualizes DATA ENGINEERING CONCEPTS, not just business KPIs.

It proves:
✅ Delta governance works (time travel, audit trail)
✅ Performance tuning delivers results (12x speedup)
✅ Metadata-driven architecture scales
✅ Data quality gates catch issues
✅ API ingestion is reliable and complete
✅ Transformations are validated and tested

Built for technical interviews to demonstrate systems thinking.
```

### Screenshot All Tabs
Capture screenshots for README:
1. `docs/screenshots/lakeview_delta_tab.png`
2. `docs/screenshots/lakeview_perf_tab.png`
3. `docs/screenshots/lakeview_metadata_tab.png`
4. `docs/screenshots/lakeview_dq_tab.png`
5. `docs/screenshots/lakeview_ingestion_tab.png`
6. `docs/screenshots/lakeview_transform_tab.png`

---

## 10. Testing Checklist

Before finalizing:
- [ ] All 30+ tiles render without errors
- [ ] All queries execute in < 5 seconds
- [ ] Conditional formatting works (red/green colors)
- [ ] Images load correctly
- [ ] HTML iframe renders Mermaid diagram
- [ ] Global filters apply to all tabs
- [ ] Mobile view is readable
- [ ] Refresh schedule is configured
- [ ] Permissions are set correctly
- [ ] Dashboard description is complete

---

## What This Dashboard Proves

**To interviewers**:

1. **Systems Thinking**: You visualize how the system works, not just what it outputs
2. **Observability**: You monitor data quality, performance, and pipeline health
3. **Documentation**: Your architecture self-documents via diagrams and metrics
4. **Production Readiness**: You think about refresh schedules, permissions, alerts
5. **Communication**: You can explain technical concepts visually to non-technical stakeholders

**Differentiation**: Most candidates show revenue dashboards. You show *how the data pipeline works*.

---

## Talking Points for Interviews

> "I built a Lakeview dashboard that visualizes data engineering concepts, not business metrics. It has 6 tabs:
>
> 1. **Delta Governance**: Shows the complete history of operations, the 17 corrupted rows we detected and deleted, and proves time travel works.
> 2. **Performance Tuning**: Displays a 12x speedup from broadcast joins, with Spark UI screenshots showing the before/after task distribution.
> 3. **Metadata Architecture**: Auto-generated diagram from YAML config, proving the join engine is scalable and self-documenting.
> 4. **Data Quality**: All quality gates are green (zero violations), mirroring the pytest test suite.
> 5. **API Ingestion**: 100% pagination completeness, FX coverage calendar, freshness tracking.
> 6. **Transforms**: Deduplication verified, size class distribution, pipeline funnel.
>
> This dashboard proves I understand how systems work, not just how to write SQL."

---

**Build Time Estimate**: 3-4 hours  
**Maintenance**: Auto-refreshes daily, self-updating from config changes  
**Impact**: HIGH (demonstrates advanced DE skills beyond typical BI)

