-- Dashboard 4: Restaurant Conversion & Upsell
-- Shows how anchor purchases drive restaurant visits

-- KPI Card 1: Overall Restaurant Conversion
WITH anchor_customers AS (
  SELECT DISTINCT b.date, b.loyalty_id
  FROM silver.baskets b
),
restaurant_visits AS (
  SELECT DISTINCT date, loyalty_id
  FROM silver.restaurant_day
)
SELECT 
  COUNT(DISTINCT CONCAT(ac.date, '-', ac.loyalty_id)) AS anchor_customer_days,
  COUNT(DISTINCT CONCAT(rv.date, '-', rv.loyalty_id)) AS restaurant_customer_days,
  ROUND(100.0 * COUNT(DISTINCT CONCAT(rv.date, '-', rv.loyalty_id)) / 
        COUNT(DISTINCT CONCAT(ac.date, '-', ac.loyalty_id)), 1) AS conversion_rate_pct
FROM anchor_customers ac
LEFT JOIN restaurant_visits rv ON ac.date = rv.date AND ac.loyalty_id = rv.loyalty_id;

-- KPI Card 2: Restaurant Revenue from Anchor Customers
SELECT 
  SUM(r.basket_value) AS total_restaurant_revenue,
  COUNT(DISTINCT r.order_id) AS total_orders,
  ROUND(AVG(r.basket_value), 2) AS avg_order_value
FROM silver.restaurant_day r
JOIN silver.baskets b ON r.date = b.date AND r.loyalty_id = b.loyalty_id;

-- Visualization 1: Conversion Trend Over Time
SELECT 
  date,
  ROUND(100.0 * restaurant_conversion, 1) AS conversion_rate_pct,
  anchor_customers,
  restaurant_customers,
  ROUND(restaurant_value, 2) AS total_restaurant_revenue
FROM gold.restaurant_conv
ORDER BY date;

-- Visualization 2: Restaurant Conversion by Anchor Family
WITH anchor_by_family AS (
  SELECT 
    b.date,
    b.loyalty_id,
    d.family AS anchor_family
  FROM silver.baskets b
  JOIN bronze.attach_transactions t ON b.receipt_id = t.receipt_id
  JOIN silver.dim_products d ON t.product_id = d.product_id
  WHERE d.size_class = 'LARGE'
  GROUP BY b.date, b.loyalty_id, d.family
),
rest_join AS (
  SELECT 
    af.anchor_family,
    COUNT(DISTINCT CONCAT(af.date, '-', af.loyalty_id)) AS anchor_customers,
    COUNT(DISTINCT CASE WHEN r.order_id IS NOT NULL 
          THEN CONCAT(af.date, '-', af.loyalty_id) END) AS restaurant_customers
  FROM anchor_by_family af
  LEFT JOIN silver.restaurant_day r ON af.date = r.date AND af.loyalty_id = r.loyalty_id
  GROUP BY af.anchor_family
)
SELECT 
  anchor_family,
  anchor_customers,
  restaurant_customers,
  ROUND(100.0 * restaurant_customers / anchor_customers, 1) AS conversion_rate_pct
FROM rest_join
ORDER BY conversion_rate_pct DESC;

-- Visualization 3: Average Restaurant Spend (Anchor vs Non-Anchor)
WITH anchor_loyals AS (
  SELECT DISTINCT loyalty_id
  FROM silver.baskets
),
restaurant_with_flag AS (
  SELECT 
    r.order_id,
    r.loyalty_id,
    r.basket_value,
    CASE WHEN al.loyalty_id IS NOT NULL THEN 'Anchor Customer' ELSE 'Non-Anchor' END AS customer_type
  FROM bronze.attach_restaurant r
  LEFT JOIN anchor_loyals al ON r.loyalty_id = al.loyalty_id
)
SELECT 
  customer_type,
  COUNT(*) AS num_orders,
  ROUND(AVG(basket_value), 2) AS avg_order_value,
  ROUND(SUM(basket_value), 2) AS total_revenue
FROM restaurant_with_flag
GROUP BY customer_type;

-- Visualization 4: Restaurant Visits by Hour (when do people eat?)
WITH restaurant_hours AS (
  SELECT 
    HOUR(ts) AS hour_of_day,
    COUNT(*) AS num_orders,
    ROUND(AVG(basket_value), 2) AS avg_order_value
  FROM bronze.attach_restaurant
  GROUP BY HOUR(ts)
)
SELECT 
  hour_of_day,
  num_orders,
  avg_order_value
FROM restaurant_hours
ORDER BY hour_of_day;

-- Analysis: Time Between Shopping and Restaurant (in minutes)
-- Shows that most restaurant visits happen 30-180 min after checkout
WITH anchor_times AS (
  SELECT 
    b.receipt_id,
    b.loyalty_id,
    b.date,
    MIN(t.ts) AS checkout_time
  FROM silver.baskets b
  JOIN bronze.attach_transactions t ON b.receipt_id = t.receipt_id
  GROUP BY b.receipt_id, b.loyalty_id, b.date
),
restaurant_with_checkout AS (
  SELECT 
    r.order_id,
    r.ts AS restaurant_time,
    at.checkout_time,
    ROUND((UNIX_TIMESTAMP(r.ts) - UNIX_TIMESTAMP(at.checkout_time)) / 60.0, 0) AS minutes_after_checkout
  FROM bronze.attach_restaurant r
  JOIN anchor_times at ON r.loyalty_id = at.loyalty_id AND TO_DATE(r.ts) = at.date
)
SELECT 
  CASE 
    WHEN minutes_after_checkout < 0 THEN 'Before Shopping'
    WHEN minutes_after_checkout BETWEEN 0 AND 30 THEN '0-30 min'
    WHEN minutes_after_checkout BETWEEN 31 AND 60 THEN '31-60 min'
    WHEN minutes_after_checkout BETWEEN 61 AND 120 THEN '61-120 min'
    WHEN minutes_after_checkout BETWEEN 121 AND 180 THEN '121-180 min'
    ELSE 'Over 180 min'
  END AS time_bucket,
  COUNT(*) AS num_orders,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 1) AS pct_of_orders
FROM restaurant_with_checkout
GROUP BY 
  CASE 
    WHEN minutes_after_checkout < 0 THEN 'Before Shopping'
    WHEN minutes_after_checkout BETWEEN 0 AND 30 THEN '0-30 min'
    WHEN minutes_after_checkout BETWEEN 31 AND 60 THEN '31-60 min'
    WHEN minutes_after_checkout BETWEEN 61 AND 120 THEN '61-120 min'
    WHEN minutes_after_checkout BETWEEN 121 AND 180 THEN '121-180 min'
    ELSE 'Over 180 min'
  END
ORDER BY 
  CASE time_bucket
    WHEN 'Before Shopping' THEN 1
    WHEN '0-30 min' THEN 2
    WHEN '31-60 min' THEN 3
    WHEN '61-120 min' THEN 4
    WHEN '121-180 min' THEN 5
    ELSE 6
  END;
