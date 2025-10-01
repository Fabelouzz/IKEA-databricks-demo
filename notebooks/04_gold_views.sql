
-- Databricks SQL
-- 04_gold_views: metrics and views for dashboard
CREATE SCHEMA IF NOT EXISTS gold;

-- Attach metrics per family of anchor
CREATE OR REPLACE VIEW gold.attach_metrics AS
WITH anchor_receipts AS (
  SELECT b.receipt_id, d.family AS anchor_family, b.date, b.loyalty_id, b.attached
  FROM silver.baskets b
  JOIN bronze.attach_transactions t ON b.receipt_id = t.receipt_id
  JOIN silver.dim_products d ON t.product_id = d.product_id
  WHERE d.size_class = 'LARGE'
  GROUP BY b.receipt_id, d.family, b.date, b.loyalty_id, b.attached
),
den AS (
  SELECT anchor_family, date, COUNT(*) AS anchor_receipts
  FROM anchor_receipts
  GROUP BY anchor_family, date
),
num AS (
  SELECT anchor_family, date, COUNT(*) AS attached_receipts
  FROM anchor_receipts
  WHERE attached = 1
  GROUP BY anchor_family, date
)
SELECT d.anchor_family,
       d.date,
       d.anchor_receipts,
       COALESCE(n.attached_receipts,0) AS attached_receipts,
       CASE WHEN d.anchor_receipts>0 THEN COALESCE(n.attached_receipts,0)/d.anchor_receipts ELSE 0 END AS attach_rate
FROM den d
LEFT JOIN num n ON d.anchor_family = n.anchor_family AND d.date = n.date;

-- Global add-on baseline probability
CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE VIEW gold.addon_baseline AS
SELECT p.product_id,
       p.family AS addon_family,
       SUM(t.qty) AS total_qty,
       SUM(SUM(t.qty)) OVER() AS total_qty_all,
       SUM(t.qty)/SUM(SUM(t.qty)) OVER() AS p_addon
FROM bronze.attach_transactions t
JOIN silver.dim_products p ON t.product_id = p.product_id
WHERE p.size_class = 'SMALL'
GROUP BY p.product_id, p.family;

-- Add-on lift per anchor family
CREATE OR REPLACE VIEW gold.addon_lift AS
WITH anchor_receipts AS (
  SELECT DISTINCT b.receipt_id, d.family AS anchor_family
  FROM silver.baskets b
  JOIN bronze.attach_transactions t ON b.receipt_id = t.receipt_id
  JOIN silver.dim_products d ON t.product_id = d.product_id
  WHERE d.size_class = 'LARGE'
),
addon_events AS (
  SELECT ar.anchor_family, d2.product_id, d2.family AS addon_family, COUNT(DISTINCT t.receipt_id) AS addon_receipts
  FROM anchor_receipts ar
  JOIN bronze.attach_transactions t ON ar.receipt_id = t.receipt_id
  JOIN silver.dim_products d2 ON t.product_id = d2.product_id
  WHERE d2.size_class = 'SMALL'
  GROUP BY ar.anchor_family, d2.product_id, d2.family
),
den AS (
  SELECT anchor_family, COUNT(DISTINCT receipt_id) AS anchor_receipts
  FROM anchor_receipts
  GROUP BY anchor_family
)
SELECT a.anchor_family, a.product_id, a.addon_family,
       a.addon_receipts, d.anchor_receipts,
       a.addon_receipts / d.anchor_receipts AS p_addon_given_anchor,
       bl.p_addon,
       CASE WHEN bl.p_addon > 0 THEN (a.addon_receipts / d.anchor_receipts) / bl.p_addon ELSE NULL END AS lift
FROM addon_events a
JOIN den d ON a.anchor_family = d.anchor_family
JOIN gold.addon_baseline bl ON a.product_id = bl.product_id;

-- Restaurant conversion among anchor customers same day
CREATE OR REPLACE VIEW gold.restaurant_conv AS
WITH anchor_loy AS (
  SELECT DISTINCT b.date, b.loyalty_id
  FROM silver.baskets b
),
rest AS (
  SELECT date, loyalty_id, COUNT(*) AS orders, SUM(basket_value) AS value
  FROM silver.restaurant_day
  GROUP BY date, loyalty_id
)
SELECT a.date,
       COUNT(*) AS anchor_customers,
       SUM(CASE WHEN r.orders IS NOT NULL THEN 1 ELSE 0 END) AS restaurant_customers,
       SUM(COALESCE(r.value,0)) AS restaurant_value,
       SUM(CASE WHEN r.orders IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) AS restaurant_conversion
FROM anchor_loy a
LEFT JOIN rest r
ON a.date = r.date AND a.loyalty_id = r.loyalty_id
GROUP BY a.date;
