-- Dashboard 2: Cross-Sell Matrix
-- Heatmap showing which anchors drive which add-ons

-- Main Visualization: Anchor × Add-on Lift Matrix
WITH anchor_receipts AS (
  SELECT DISTINCT 
    t.receipt_id, 
    d.family AS anchor_family
  FROM bronze.attach_transactions t
  JOIN silver.dim_products d ON t.product_id = d.product_id
  WHERE d.size_class = 'LARGE'
),
addon_events AS (
  SELECT 
    ar.anchor_family,
    d2.family AS addon_family,
    COUNT(DISTINCT t.receipt_id) AS addon_receipts
  FROM anchor_receipts ar
  JOIN bronze.attach_transactions t ON ar.receipt_id = t.receipt_id
  JOIN silver.dim_products d2 ON t.product_id = d2.product_id
  WHERE d2.size_class = 'SMALL'
  GROUP BY ar.anchor_family, d2.family
),
anchor_counts AS (
  SELECT anchor_family, COUNT(*) AS anchor_count
  FROM anchor_receipts
  GROUP BY anchor_family
),
addon_baseline AS (
  SELECT 
    d.family AS addon_family,
    COUNT(DISTINCT t.receipt_id) AS total_receipts_with_addon,
    (SELECT COUNT(DISTINCT receipt_id) FROM bronze.attach_transactions) AS total_receipts
  FROM bronze.attach_transactions t
  JOIN silver.dim_products d ON t.product_id = d.product_id
  WHERE d.size_class = 'SMALL'
  GROUP BY d.family
)
SELECT 
  ae.anchor_family,
  ae.addon_family,
  ae.addon_receipts,
  ac.anchor_count,
  ROUND(100.0 * ae.addon_receipts / ac.anchor_count, 1) AS attach_rate_pct,
  ROUND(100.0 * ab.total_receipts_with_addon / ab.total_receipts, 1) AS baseline_rate_pct,
  ROUND((ae.addon_receipts / ac.anchor_count) / (ab.total_receipts_with_addon / ab.total_receipts), 2) AS lift
FROM addon_events ae
JOIN anchor_counts ac ON ae.anchor_family = ac.anchor_family
JOIN addon_baseline ab ON ae.addon_family = ab.addon_family
ORDER BY ae.anchor_family, lift DESC;

-- Table 1: Top 10 Add-ons per Anchor Family
WITH ranked_addons AS (
  SELECT 
    anchor_family,
    product_id,
    addon_family,
    lift,
    p_addon_given_anchor,
    ROW_NUMBER() OVER (PARTITION BY anchor_family ORDER BY lift DESC) AS rank
  FROM gold.addon_lift
)
SELECT 
  anchor_family,
  addon_family,
  product_id,
  ROUND(lift, 2) AS lift,
  ROUND(100.0 * p_addon_given_anchor, 1) AS attach_rate_pct
FROM ranked_addons
WHERE rank <= 10
ORDER BY anchor_family, rank;

-- KPI Card: Overall Attach Rate by Anchor Family
SELECT 
  anchor_family AS `Anchor Family`,
  SUM(anchor_receipts) AS `Total Anchor Receipts`,
  SUM(attached_receipts) AS `Total Attached Receipts`,
  ROUND(100.0 * SUM(attached_receipts) / SUM(anchor_receipts), 1) AS `Overall Attach Rate (%)`
FROM gold.attach_metrics
GROUP BY anchor_family
ORDER BY `Overall Attach Rate (%)` DESC;

-- Trend: Attach Rate Over Time by Anchor
SELECT 
  date,
  anchor_family,
  ROUND(100.0 * attach_rate, 1) AS attach_rate_pct
FROM gold.attach_metrics
WHERE date >= DATE_SUB(CURRENT_DATE(), 30)
ORDER BY date, anchor_family;

-- Analysis: Underperforming Anchors (low attach rate)
WITH avg_attach AS (
  SELECT AVG(attach_rate) AS avg_rate
  FROM gold.attach_metrics
)
SELECT 
  m.anchor_family,
  ROUND(100.0 * AVG(m.attach_rate), 1) AS attach_rate_pct,
  ROUND(100.0 * aa.avg_rate, 1) AS avg_attach_rate_pct,
  CASE 
    WHEN AVG(m.attach_rate) < aa.avg_rate THEN 'Below Average'
    ELSE 'Above Average'
  END AS performance
FROM gold.attach_metrics m, avg_attach aa
GROUP BY m.anchor_family, aa.avg_rate
ORDER BY attach_rate_pct;
