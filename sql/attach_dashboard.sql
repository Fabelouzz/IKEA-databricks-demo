
-- Attach dashboard example queries

-- 1) Attach rate trend per anchor family
SELECT date, anchor_family, attach_rate
FROM gold.attach_metrics
WHERE date >= date_sub(current_date(), 30)
ORDER BY date, anchor_family;

-- 2) Top 10 add-ons by lift for SOFAS
SELECT addon_family, product_id, lift, p_addon_given_anchor, p_addon
FROM gold.addon_lift
WHERE anchor_family = 'SOFAS'
ORDER BY lift DESC
LIMIT 10;

-- 3) Restaurant conversion trend
SELECT date, restaurant_conversion, restaurant_value
FROM gold.restaurant_conv
ORDER BY date;
