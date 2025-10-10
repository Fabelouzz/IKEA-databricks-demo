## Interview Add-on: High-Impact, Feasible Extensions for This Repo (48h)

### TL;DR
- **Most valuable to implement now**: API ingestion to bronze, CTE-driven silver SQL with keys, metadata-driven joins to gold, Delta time travel demo, and a small Spark skew vs broadcast performance study, plus 1–2 data tests.
- **Why**: These prove practical data engineering skills (ingestion, modeling, architecture, reliability, performance) and integrate cleanly with the repo’s existing bronze → silver → gold and dbt layout.
- **What changes**: Add 5 small notebooks/scripts, 1 YAML config, minimal tests, and 2–3 screenshots; link them from the README.

---

### Current State (What We’re Building On)
- Notebooks already cover: data generation, bronze load, silver transforms, gold views, and 2 ML notebooks.
- dbt project exists with `staging` and `marts` (schemas map to `silver` and `gold`).
- Dashboards and visuals are included; `databricks.yml` bundle exists for easy workspace deployment.

Implication: We can add targeted artifacts without reworking the architecture. New pieces should slot into the medallion layers and reuse conventions (`bronze.*`, `silver.*`, `gold.*`).

---

### Prioritized Additions (Impact × Feasibility)

1) API Ingestion to Bronze (APIs & Ingestion)
- Objective: Demonstrate robust external data ingestion with validation, pagination, and schema control.
- Integration:
  - New file: `notebooks/01_ingest_api_fx.py` (keeps numeric prefix pattern).
  - Ingest a small public FX or rates API to `bronze.fx_rates` with columns: `pair`, `rate`, `as_of`, `ingested_at`, `source`.
  - Keep retries/timeouts; land as Delta with explicit schema (no infer).
  - Add exported requests to `ops/postman_collection.json` and a README screenshot.
- Why it matters: Shows API GET vs POST reasoning, validation-before-coding, pagination, and landing to bronze with contracts.
- Feasibility risks: Local runs may need `requests/httpx`. If running in Databricks, the default environment suffices; otherwise, extend `requirements.txt` for local dev.

2) CTE-Driven Silver SQL with Keys (SQL Modeling)
- Objective: Show clean SQL reasoning, CTEs, and primary/composite key intent.
- Integration:
  - New file: `notebooks/02_silver_transform_sql.sql` or `sql/02_silver_transform_sql.sql`.
  - Create `silver.fx_rates_daily` from `bronze.fx_rates` via CTEs (`clean_rates`, `deduped`, `latest_by_day`).
  - Document PK intent in comments (e.g., `(pair, as_of_date)`), and join a tiny `dim_currency` if present or create a minimal dim.
- Why it matters: Interviewers read CTE structure to assess data modeling clarity and key semantics.
- Feasibility: Pure SQL, minimal coupling to existing models; aligns with repo’s medallion approach.

3) Metadata-Driven Joins to Gold (Architecture & Maintainability)
- Objective: Prove scalable, config-based enrichment without bespoke join code per table.
- Integration:
  - New file: `config/joins.yml` to describe base, joins, and broadcast hints.
  - New file: `notebooks/03_metadata_joins.py` that loads YAML, applies joins generically (Spark SQL `expr(on)`), and writes `gold.orders_enriched`.
  - Suggested base: an existing silver table (e.g., `silver.baskets` or `silver.transactions`); include a join to `silver.fx_rates_daily` for currency normalization.
- Why it matters: Demonstrates architecture thinking and maintainability (add 10–20 joins via config, not code).
- Feasibility: Self-contained; reads/writes within current schemas. No dependency on ML components.

4) Delta Time Travel & Corrections (Governance & Reliability)
- Objective: Show practical Delta operations: delete, history, rollback/time travel.
- Integration:
  - New file: `notebooks/04_delta_time_travel.py` operating on `gold.orders_enriched`.
  - Steps: insert a known bad row, delete by predicate, show `DESCRIBE HISTORY`, read `versionAsOf` to demonstrate time travel, optionally revert.
  - Capture a screenshot of the history output for README.
- Why it matters: Auditable changes and recovery are core for production DE.
- Feasibility: Works out-of-the-box on Delta tables created above.

5) Spark Performance: Skew vs Broadcast (Performance Engineering)
- Objective: Reproduce skew, show AQE/broadcast fix, and reference Spark UI.
- Integration:
  - New file: `notebooks/05_perf_skew_vs_broadcast.py` creating a skewed mini dataset and a naive join vs. AQE+broadcast.
  - Print `explain("formatted")`, collect timings, and include 2 Spark UI screenshots (skewed vs improved).
- Why it matters: Shows you can read Spark stages and fix real-world pain points.
- Feasibility: Self-contained synthetic demo; no external dependencies.

6) Minimal Data Tests (Quality & Contracts)
- Objective: Prove you test data logic, not just code paths.
- Integration:
  - New file: `tests/test_transforms.py` with PySpark unit tests for the silver FX daily logic (no null PKs, dedup correctness, sensible rate bounds).
  - Optional: `tests/expectations/` for a tiny Great Expectations suite on `silver.fx_rates_daily` (dup check on `(pair, as_of_date)`).
- Why it matters: Treat pipelines like software, surface quality gates.
- Feasibility: Keep minimal; add `pytest` and `chispa` locally if needed.

---

### Concrete File/Artifact Additions
- `notebooks/01_ingest_api_fx.py`
- `notebooks/02_silver_transform_sql.sql`
- `config/joins.yml`
- `notebooks/03_metadata_joins.py`
- `notebooks/04_delta_time_travel.py`
- `notebooks/05_perf_skew_vs_broadcast.py`
- `tests/test_transforms.py`
- `ops/postman_collection.json` (exported)

Optional (if time/creds):
- `tests/expectations/` (Great Expectations mini suite)
- `ops/powerbi_refresh.py` (stub for BI refresh)

README updates:
- Add a “48-hour interview add-on” section with: run order (01→05), skill mapping table, and 2–3 screenshots (Postman validation, Spark UI before/after, Delta history).

---

### Integration Details and Naming
- Schemas: Continue using `bronze.*`, `silver.*`, `gold.*` to match the README and SQL assets.
- Table names:
  - `bronze.fx_rates`
  - `silver.fx_rates_daily`
  - `gold.orders_enriched` (or `gold.baskets_enriched` if using `silver.baskets` as base)
- dbt: Keep dbt focused on existing staging/marts. The new silver/gold artifacts can later be templated into dbt if desired, but not required for the 48h add-on.
- Bundles: No change required to `databricks.yml`; these notebooks can be run ad hoc or wired into a Job later.

---

### Dependencies and Environment Notes
- Current `requirements.txt` is minimal (PySpark + Databricks Connect). For local runs (optional):
  - Add `requests` or `httpx` for API ingestion.
  - Add `pytest` and `chispa` for data tests.
  - Great Expectations only if you choose to include expectations.
  - On Databricks, you can install libraries via cluster UI or `%pip` in the notebook.

---

### Deliverables You Can Show Interviewers
- Code artifacts (files above) + concise README section mapping skills → artifacts.
- Postman collection proving API validation and contracts.
- 2–3 screenshots: Postman call, Spark UI skew vs fixed, `DESCRIBE HISTORY` output.
- One or two passing data tests demonstrating dedup/PK logic.

---

### Suggested Run Order
1. `01_ingest_api_fx.py` → creates `bronze.fx_rates`.
2. `02_silver_transform_sql.sql` → creates `silver.fx_rates_daily`.
3. `03_metadata_joins.py` → produces `gold.orders_enriched`.
4. `04_delta_time_travel.py` → demonstrates governance and recovery.
5. `05_perf_skew_vs_broadcast.py` → demonstrates performance tuning.
6. `tests/test_transforms.py` → run locally or in a CI step, optional.

---

### Why These Five First
- They hit the most common interview probes for DE roles: ingestion patterns, SQL modeling clarity, scalable architecture, reliability/governance, and performance tuning. Each is self-contained, quick to implement, and integrates naturally with this repo’s medallion and Databricks setup.


