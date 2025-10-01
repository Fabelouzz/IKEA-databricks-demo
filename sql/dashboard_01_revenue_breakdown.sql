-- Dashboard 1: Revenue Breakdown (Executive View)
-- Shows that cross-selling drives IKEA's profitability

-- KPI Card 1: Total Revenue by Type
WITH revenue_by_type AS (
  SELECT 
    SUM(CASE WHEN d.size_class = 'LARGE' THEN d.price * t.qty ELSE 0 END) AS anchor_revenue,
    SUM(CASE WHEN d.size_class = 'SMALL' THEN d.price * t.qty ELSE 0 END) AS addon_revenue,
    COUNT(DISTINCT t.receipt_id) AS total_receipts
  FROM bronze.attach_transactions t
  JOIN silver.dim_products d ON t.product_id = d.product_id
),
restaurant_revenue AS (
  SELECT SUM(basket_value) AS restaurant_revenue
  FROM bronze.attach_restaurant
)
SELECT 
  anchor_revenue,
  addon_revenue,
  restaurant_revenue,
  (addon_revenue + anchor_revenue + restaurant_revenue) AS total_revenue,
  -- Store as decimal (0.384) so Databricks % format shows 38.4%
  ROUND(addon_revenue / (addon_revenue + anchor_revenue), 3) AS pct_from_addons,
  ROUND((addon_revenue + anchor_revenue + restaurant_revenue) / total_receipts, 2) AS avg_basket_value
FROM revenue_by_type, restaurant_revenue;

-- Visualization 1: Daily Revenue Waterfall (Anchor → Add-on → Restaurant)
SELECT 
  TO_DATE(t.ts) AS date,
  SUM(CASE WHEN d.size_class = 'LARGE' THEN d.price * t.qty ELSE 0 END) AS `Anchor Revenue`,
  SUM(CASE WHEN d.size_class = 'SMALL' THEN d.price * t.qty ELSE 0 END) AS `Add-on Revenue`
FROM bronze.attach_transactions t
JOIN silver.dim_products d ON t.product_id = d.product_id
GROUP BY TO_DATE(t.ts)
ORDER BY date;

-- Visualization 2: Margin Contribution by Category
SELECT 
  d.size_class,
  SUM(d.margin * t.qty) AS total_margin,
  SUM(d.price * t.qty) AS total_revenue,
  ROUND(100.0 * SUM(d.margin * t.qty) / SUM(SUM(d.margin * t.qty)) OVER(), 1) AS pct_of_total_margin
FROM bronze.attach_transactions t
JOIN silver.dim_products d ON t.product_id = d.product_id
GROUP BY d.size_class;

-- KPI Card 2: Anchor vs Non-Anchor Basket Value
WITH anchor_receipts AS (
  SELECT DISTINCT receipt_id
  FROM bronze.attach_transactions t
  JOIN silver.dim_products d ON t.product_id = d.product_id
  WHERE d.size_class = 'LARGE'
),
basket_values AS (
  SELECT 
    t.receipt_id,
    CASE WHEN ar.receipt_id IS NOT NULL THEN 'Anchor' ELSE 'Non-Anchor' END AS basket_type,
    SUM(d.price * t.qty) AS basket_value,
    COUNT(DISTINCT t.product_id) AS item_count
  FROM bronze.attach_transactions t
  JOIN silver.dim_products d ON t.product_id = d.product_id
  LEFT JOIN anchor_receipts ar ON t.receipt_id = ar.receipt_id
  GROUP BY t.receipt_id, basket_type
)
SELECT 
  basket_type,
  COUNT(*) AS num_baskets,
  ROUND(AVG(basket_value), 2) AS avg_basket_value,
  ROUND(AVG(item_count), 1) AS avg_item_count
FROM basket_values
GROUP BY basket_type;

-- Visualization 3: Revenue Per Visitor vs Revenue Per Anchor Customer
WITH daily_stats AS (
  SELECT 
    TO_DATE(t.ts) AS date,
    COUNT(DISTINCT t.receipt_id) AS total_receipts,
    SUM(d.price * t.qty) AS total_revenue
  FROM bronze.attach_transactions t
  JOIN silver.dim_products d ON t.product_id = d.product_id
  GROUP BY TO_DATE(t.ts)
),
anchor_stats AS (
  SELECT 
    b.date,
    COUNT(*) AS anchor_receipts,
    SUM(d.price * t.qty) AS anchor_customer_revenue
  FROM silver.baskets b
  JOIN bronze.attach_transactions t ON b.receipt_id = t.receipt_id
  JOIN silver.dim_products d ON t.product_id = d.product_id
  GROUP BY b.date
)
SELECT 
  d.date,
  ROUND(d.total_revenue / d.total_receipts, 2) AS revenue_per_receipt,
  ROUND(a.anchor_customer_revenue / a.anchor_receipts, 2) AS revenue_per_anchor_customer,
  a.anchor_receipts,
  d.total_receipts
FROM daily_stats d
JOIN anchor_stats a ON d.date = a.date
ORDER BY d.date;
