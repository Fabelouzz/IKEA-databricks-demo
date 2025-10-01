-- Dashboard 5: ML Model Performance & Metrics
-- Shows confusion matrices, AUC, accuracy, precision, recall for both ML models

-- ============================================================================
-- SECTION 1: Restaurant Propensity Model Performance
-- ============================================================================

-- KPI Card 1: Restaurant Model Summary
SELECT 
  'Restaurant Propensity' AS model_name,
  COUNT(*) AS total_predictions,
  ROUND(AVG(propensity_score), 3) AS avg_propensity,
  ROUND(MIN(propensity_score), 3) AS min_propensity,
  ROUND(MAX(propensity_score), 3) AS max_propensity,
  ROUND(AVG(CASE WHEN visited_restaurant = 1 THEN propensity_score END), 3) AS avg_score_actual_yes,
  ROUND(AVG(CASE WHEN visited_restaurant = 0 THEN propensity_score END), 3) AS avg_score_actual_no
FROM gold.restaurant_propensity_scores;

-- Visualization 1: Propensity Score Distribution (Histogram)
-- For Databricks SQL: Use this as source for histogram visualization
SELECT 
  FLOOR(propensity_score * 10) / 10 AS score_bucket,
  COUNT(*) AS num_receipts,
  ROUND(AVG(visited_restaurant), 3) AS actual_conversion_rate
FROM gold.restaurant_propensity_scores
GROUP BY FLOOR(propensity_score * 10) / 10
ORDER BY score_bucket;

-- Visualization 2: Lift Curve (Decile Analysis)
WITH deciles AS (
  SELECT 
    propensity_score,
    visited_restaurant,
    NTILE(10) OVER (ORDER BY propensity_score DESC) AS decile
  FROM gold.restaurant_propensity_scores
),
baseline AS (
  SELECT AVG(visited_restaurant) AS baseline_rate
  FROM gold.restaurant_propensity_scores
)
SELECT 
  d.decile,
  COUNT(*) AS num_receipts,
  SUM(visited_restaurant) AS actual_visits,
  ROUND(AVG(visited_restaurant), 3) AS conversion_rate,
  ROUND(AVG(propensity_score), 3) AS avg_score,
  ROUND(AVG(visited_restaurant) / b.baseline_rate, 2) AS lift
FROM deciles d, baseline b
GROUP BY d.decile, b.baseline_rate
ORDER BY d.decile;

-- KPI Card 2: Top 20% Performance
WITH top_20 AS (
  SELECT 
    propensity_score,
    visited_restaurant
  FROM gold.restaurant_propensity_scores
  ORDER BY propensity_score DESC
  LIMIT (SELECT CAST(COUNT(*) * 0.2 AS INT) FROM gold.restaurant_propensity_scores)
),
baseline AS (
  SELECT AVG(visited_restaurant) AS baseline_rate
  FROM gold.restaurant_propensity_scores
)
SELECT 
  'Top 20% by Score' AS segment,
  COUNT(*) AS num_receipts,
  SUM(visited_restaurant) AS actual_visits,
  ROUND(AVG(visited_restaurant), 3) AS conversion_rate,
  b.baseline_rate,
  ROUND(AVG(visited_restaurant) / b.baseline_rate, 2) AS lift
FROM top_20, baseline b
GROUP BY b.baseline_rate;

-- ============================================================================
-- SECTION 2: Add-on Category Model Performance
-- ============================================================================

-- Table: Category Model Performance Summary
SELECT 
  category,
  ROUND(auc, 3) AS auc,
  ROUND(accuracy, 3) AS accuracy,
  ROUND(precision, 3) AS precision,
  ROUND(recall, 3) AS recall,
  ROUND(f1, 3) AS f1_score,
  ROUND(baseline_rate, 3) AS baseline_rate,
  ROUND(lift, 2) AS lift_top_20
FROM gold.addon_propensity_model_performance
ORDER BY auc DESC;

-- Visualization 3: Model Performance Comparison (Bar Chart)
-- AUC by Category
SELECT 
  category,
  ROUND(auc, 3) AS auc,
  ROUND(baseline_rate, 3) AS baseline_rate,
  CASE 
    WHEN auc >= 0.75 THEN 'Excellent'
    WHEN auc >= 0.70 THEN 'Good'
    WHEN auc >= 0.65 THEN 'Acceptable'
    ELSE 'Needs Improvement'
  END AS performance_tier
FROM gold.addon_propensity_model_performance
ORDER BY auc DESC;

-- ============================================================================
-- SECTION 3: Business Impact Simulation
-- ============================================================================

-- KPI Card 3: Restaurant Revenue Impact
WITH high_propensity AS (
  SELECT 
    r.receipt_id,
    r.propensity_score,
    t.ts,
    t.loyalty_id
  FROM gold.restaurant_propensity_scores r
  JOIN bronze.attach_transactions t ON r.receipt_id = t.receipt_id
  WHERE r.propensity_score >= 0.6  -- High propensity threshold
  GROUP BY r.receipt_id, r.propensity_score, t.ts, t.loyalty_id
),
restaurant_value AS (
  SELECT 
    hp.receipt_id,
    rest.basket_value
  FROM high_propensity hp
  JOIN bronze.attach_restaurant rest 
    ON TO_DATE(hp.ts) = TO_DATE(rest.ts) 
    AND hp.loyalty_id = rest.loyalty_id
)
SELECT 
  COUNT(DISTINCT hp.receipt_id) AS high_propensity_receipts,
  COUNT(DISTINCT rv.receipt_id) AS actual_restaurant_visits,
  ROUND(COUNT(DISTINCT rv.receipt_id) / COUNT(DISTINCT hp.receipt_id), 3) AS conversion_rate,
  ROUND(SUM(rv.basket_value), 2) AS total_restaurant_revenue,
  ROUND(AVG(rv.basket_value), 2) AS avg_restaurant_spend,
  ROUND(SUM(rv.basket_value) * 0.10, 2) AS estimated_revenue_if_10pct_lift
FROM high_propensity hp
LEFT JOIN restaurant_value rv ON hp.receipt_id = rv.receipt_id;

-- ============================================================================
-- SECTION 4: Anchor-Specific Add-on Recommendations
-- ============================================================================

-- Table: Top Recommendations by Anchor Type
SELECT 
  anchor_family,
  top_1 AS top_recommendation,
  ROUND(top_1_rate * 100, 1) AS attach_rate_pct,
  top_2 AS second_recommendation,
  ROUND(top_2_rate * 100, 1) AS second_rate_pct,
  top_3 AS third_recommendation,
  ROUND(top_3_rate * 100, 1) AS third_rate_pct
FROM gold.addon_recommendations
ORDER BY anchor_family;

-- ============================================================================
-- SECTION 5: Add-on Recommendations by Anchor Family (Actionable Matrix)
-- ============================================================================

-- Visualization: Top Add-on Recommendations by Anchor Type
SELECT 
  anchor_family AS `Anchor Family`,
  top_1 AS `Top Recommendation`,
  ROUND(top_1_rate, 3) AS `Top Attach Rate`,
  top_2 AS `Second Recommendation`,
  ROUND(top_2_rate, 3) AS `Second Attach Rate`,
  top_3 AS `Third Recommendation`,
  ROUND(top_3_rate, 3) AS `Third Attach Rate`
FROM gold.addon_recommendations
ORDER BY `Anchor Family`;

-- Visualization: Add-on Category Performance Comparison (Bar Chart)
SELECT 
  category AS `Category`,
  ROUND(auc, 3) AS `AUC`,
  CASE 
    WHEN auc >= 0.75 THEN 'Excellent'
    WHEN auc >= 0.70 THEN 'Good'
    WHEN auc >= 0.65 THEN 'Acceptable'
    ELSE 'Needs Improvement'
  END AS `Performance Tier`,
  ROUND(lift, 2) AS `Lift (Top 20%)`,
  ROUND(baseline_rate, 3) AS `Baseline Rate`
FROM gold.addon_propensity_model_performance
ORDER BY `AUC` DESC;

-- ============================================================================
-- SECTION 6: Feature Importance (Manual - From MLflow)
-- ============================================================================

-- Note: Feature importance is logged in MLflow
-- To create a dashboard visualization:
-- 1. Go to MLflow UI
-- 2. Find the latest restaurant_propensity run
-- 3. Download feature importance metrics
-- 4. Create a manual table or import to Databricks SQL

-- Template for manual feature importance table:
CREATE OR REPLACE TEMP VIEW restaurant_feature_importance AS
SELECT * FROM (VALUES
  ('item_count', 0.35),
  ('basket_value', 0.22),
  ('household_size', 0.18),
  ('is_afternoon', 0.12),
  ('addon_count', 0.08),
  ('is_weekend', 0.05)
) AS t(feature, importance)
ORDER BY importance DESC;

-- Visualization: Feature Importance Bar Chart
SELECT 
  feature,
  ROUND(importance, 3) AS importance,
  ROUND(importance * 100, 1) AS importance_pct
FROM restaurant_feature_importance
ORDER BY importance DESC;

-- ============================================================================
-- SECTION 7: Model Calibration Check
-- ============================================================================

-- Check if predicted probabilities match actual rates
WITH score_buckets AS (
  SELECT 
    CASE 
      WHEN propensity_score < 0.2 THEN '0.0-0.2'
      WHEN propensity_score < 0.4 THEN '0.2-0.4'
      WHEN propensity_score < 0.6 THEN '0.4-0.6'
      WHEN propensity_score < 0.8 THEN '0.6-0.8'
      ELSE '0.8-1.0'
    END AS predicted_range,
    propensity_score,
    visited_restaurant
  FROM gold.restaurant_propensity_scores
)
SELECT 
  predicted_range,
  COUNT(*) AS num_receipts,
  ROUND(AVG(propensity_score), 3) AS avg_predicted_prob,
  ROUND(AVG(visited_restaurant), 3) AS actual_rate,
  ROUND(ABS(AVG(propensity_score) - AVG(visited_restaurant)), 3) AS calibration_error
FROM score_buckets
GROUP BY predicted_range
ORDER BY predicted_range;

-- ============================================================================
-- SECTION 8: Confusion Matrix Summary (From MLflow Metrics)
-- ============================================================================

-- Note: Actual confusion matrix values are in MLflow
-- Template for creating a visual summary:
CREATE OR REPLACE TEMP VIEW model_confusion_summary AS
SELECT * FROM (VALUES
  ('Restaurant Propensity', 'Good', 0.75, 'See MLflow for full matrix'),
  ('CHAIRS Category', 'Good', 0.70, 'TP=0, TN=5529, FP=0, FN=2158'),
  ('LAMPS Category', 'Acceptable', 0.66, 'TP=3583, TN=1458, FP=2181, FN=465'),
  ('TEXTILES Category', 'Acceptable', 0.65, 'See MLflow for full matrix')
) AS t(model_name, performance, auc, confusion_matrix_note);

SELECT * FROM model_confusion_summary
ORDER BY auc DESC;
