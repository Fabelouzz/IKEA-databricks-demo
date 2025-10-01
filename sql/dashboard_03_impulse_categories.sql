-- Dashboard 3: Impulse Category Performance
-- Focus on DECOR, KITCHENWARE, LAMPS (checkout zone items)

-- KPI Cards: Impulse Category Attach Rates
WITH anchor_receipts AS (
  SELECT DISTINCT receipt_id
  FROM bronze.attach_transactions t
  JOIN silver.dim_products d ON t.product_id = d.product_id
  WHERE d.size_class = 'LARGE'
),
impulse_attach AS (
  SELECT 
    d.family,
    COUNT(DISTINCT t.receipt_id) AS receipts_with_impulse
  FROM bronze.attach_transactions t
  JOIN silver.dim_products d ON t.product_id = d.product_id
  JOIN anchor_receipts ar ON t.receipt_id = ar.receipt_id
  WHERE d.family IN ('DECOR', 'KITCHENWARE', 'LAMPS')
  GROUP BY d.family
)
SELECT 
  ia.family AS impulse_category,
  ia.receipts_with_impulse,
  (SELECT COUNT(*) FROM anchor_receipts) AS total_anchor_receipts,
  ROUND(100.0 * ia.receipts_with_impulse / (SELECT COUNT(*) FROM anchor_receipts), 1) AS attach_rate_pct
FROM impulse_attach ia
ORDER BY attach_rate_pct DESC;

-- Visualization 1: Impulse Items by Hour of Day
-- (tired shoppers buy more impulse items in the afternoon/evening)
WITH anchor_receipts AS (
  SELECT DISTINCT t.receipt_id, t.ts
  FROM bronze.attach_transactions t
  JOIN silver.dim_products d ON t.product_id = d.product_id
  WHERE d.size_class = 'LARGE'
),
impulse_by_hour AS (
  SELECT 
    HOUR(ar.ts) AS hour_of_day,
    d.family,
    COUNT(DISTINCT t.receipt_id) AS impulse_receipts
  FROM bronze.attach_transactions t
  JOIN silver.dim_products d ON t.product_id = d.product_id
  JOIN anchor_receipts ar ON t.receipt_id = ar.receipt_id
  WHERE d.family IN ('DECOR', 'KITCHENWARE', 'LAMPS')
  GROUP BY HOUR(ar.ts), d.family
),
anchor_by_hour AS (
  SELECT HOUR(ts) AS hour_of_day, COUNT(*) AS anchor_count
  FROM anchor_receipts
  GROUP BY HOUR(ts)
)
SELECT 
  ibh.hour_of_day,
  ibh.family,
  ibh.impulse_receipts,
  abh.anchor_count,
  ROUND(100.0 * ibh.impulse_receipts / abh.anchor_count, 1) AS attach_rate_pct
FROM impulse_by_hour ibh
JOIN anchor_by_hour abh ON ibh.hour_of_day = abh.hour_of_day
ORDER BY ibh.hour_of_day, ibh.family;

-- Visualization 2: Average Impulse Items per Basket (Trend)
WITH anchor_baskets AS (
  SELECT DISTINCT 
    TO_DATE(t.ts) AS date,
    t.receipt_id
  FROM bronze.attach_transactions t
  JOIN silver.dim_products d ON t.product_id = d.product_id
  WHERE d.size_class = 'LARGE'
),
impulse_counts AS (
  SELECT 
    TO_DATE(t.ts) AS date,
    t.receipt_id,
    COUNT(*) AS impulse_item_count
  FROM bronze.attach_transactions t
  JOIN silver.dim_products d ON t.product_id = d.product_id
  WHERE d.family IN ('DECOR', 'KITCHENWARE', 'LAMPS')
  GROUP BY TO_DATE(t.ts), t.receipt_id
)
SELECT 
  ab.date,
  COUNT(DISTINCT ab.receipt_id) AS anchor_receipts,
  COALESCE(SUM(ic.impulse_item_count), 0) AS total_impulse_items,
  ROUND(COALESCE(SUM(ic.impulse_item_count), 0) / COUNT(DISTINCT ab.receipt_id), 2) AS avg_impulse_per_basket
FROM anchor_baskets ab
LEFT JOIN impulse_counts ic ON ab.date = ic.date AND ab.receipt_id = ic.receipt_id
GROUP BY ab.date
ORDER BY ab.date;

-- Table: Unrelated Item Purchases
-- Baskets with items from 3+ unrelated families (shows impulse buying)
WITH basket_families AS (
  SELECT 
    t.receipt_id,
    COUNT(DISTINCT d.family) AS family_count,
    COLLECT_SET(d.family) AS families
  FROM bronze.attach_transactions t
  JOIN silver.dim_products d ON t.product_id = d.product_id
  GROUP BY t.receipt_id
)
SELECT 
  family_count,
  COUNT(*) AS num_baskets,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct_of_baskets
FROM basket_families
WHERE family_count >= 1
GROUP BY family_count
ORDER BY family_count;

-- Analysis: Top Impulse Items (by absolute sales)
SELECT 
  d.family,
  d.product_id,
  d.sku,
  d.price,
  SUM(t.qty) AS total_qty_sold,
  COUNT(DISTINCT t.receipt_id) AS times_purchased
FROM bronze.attach_transactions t
JOIN silver.dim_products d ON t.product_id = d.product_id
WHERE d.family IN ('DECOR', 'KITCHENWARE', 'LAMPS')
GROUP BY d.family, d.product_id, d.sku, d.price
ORDER BY total_qty_sold DESC
LIMIT 20;
