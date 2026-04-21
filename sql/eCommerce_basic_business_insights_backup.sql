-- =========================================
-- ECOMMERCE ANALYTICS - PROFESSIONAL VERSION
-- =========================================

-- 1. Preview Data
SELECT *
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
LIMIT 10;


-- =========================================
-- CORE BUSINESS METRICS
-- =========================================

-- 2. Total Revenue
SELECT 
  ROUND(SUM(order_value), 2) AS total_revenue
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`;


-- 3. Total Orders
SELECT 
  COUNT(DISTINCT order_id) AS total_orders
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`;


-- 4. Unique Customers
SELECT 
  COUNT(DISTINCT customer_id) AS unique_customers
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`;


-- 5. Total Items Sold
SELECT 
  SUM(quantity) AS total_items_sold
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`;


-- 6. Average Order Value (AOV)
SELECT 
  ROUND(SUM(order_value) / COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`;


-- =========================================
-- TIME-BASED ANALYSIS
-- =========================================

-- 7. Monthly Revenue Trend (Proper DATE extraction)
SELECT 
  FORMAT_TIMESTAMP('%Y-%m', TIMESTAMP(order_timestamp)) AS month,
  ROUND(SUM(order_value), 2) AS monthly_revenue,
  COUNT(DISTINCT order_id) AS total_orders
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
GROUP BY month
ORDER BY month;


-- 8. Daily Revenue Trend (for dashboard granularity)
SELECT 
  DATE(TIMESTAMP(order_timestamp)) AS order_date,
  ROUND(SUM(order_value), 2) AS daily_revenue
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
GROUP BY order_date
ORDER BY order_date;


-- =========================================
-- PRODUCT PERFORMANCE
-- =========================================

-- 9. Top 10 Best-Selling Products (with revenue)
SELECT 
  product_id,
  SUM(quantity) AS total_quantity_sold,
  ROUND(SUM(order_value), 2) AS total_revenue
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
GROUP BY product_id
ORDER BY total_quantity_sold DESC
LIMIT 10;


-- 10. Top Products by Revenue
SELECT 
  product_id,
  ROUND(SUM(order_value), 2) AS total_revenue
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 10;


-- =========================================
-- CUSTOMER ANALYSIS
-- =========================================

-- 11. Top 10 Customers by Spending
SELECT 
  customer_id,
  ROUND(SUM(order_value), 2) AS total_spent,
  COUNT(order_id) AS total_orders
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 10;


-- 12. Customer Segmentation (High / Medium / Low)
SELECT 
  customer_id,
  total_spent,
  CASE 
    WHEN total_spent > 50000 THEN 'High Value'
    WHEN total_spent BETWEEN 20000 AND 50000 THEN 'Medium Value'
    ELSE 'Low Value'
  END AS customer_segment
FROM (
  SELECT 
    customer_id,
    SUM(order_value) AS total_spent
  FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
  GROUP BY customer_id
);


-- =========================================
-- ORDER STATUS ANALYSIS
-- =========================================

-- 13. Order Status Distribution
SELECT 
  status,
  COUNT(*) AS total_orders,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
GROUP BY status;


-- =========================================
-- PAYMENT METHOD ANALYSIS
-- =========================================

-- 14. Payment Method Usage
SELECT 
  payment_method,
  COUNT(*) AS total_orders,
  ROUND(SUM(order_value), 2) AS total_revenue
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
GROUP BY payment_method
ORDER BY total_orders DESC;


-- =========================================
-- ADVANCED INSIGHT (INTERVIEW LEVEL)
-- =========================================

-- 15. Repeat vs New Customers
WITH customer_orders AS (
  SELECT 
    customer_id,
    COUNT(order_id) AS order_count
  FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
  GROUP BY customer_id
)

SELECT 
  CASE 
    WHEN order_count = 1 THEN 'New Customer'
    ELSE 'Repeat Customer'
  END AS customer_type,
  COUNT(*) AS total_customers
FROM customer_orders
GROUP BY customer_type;


-- 16. Revenue Contribution by Customer Type
WITH customer_orders AS (
  SELECT 
    customer_id,
    COUNT(order_id) AS order_count,
    SUM(order_value) AS total_spent
  FROM `trans-serenity-478906-p9.ecommerce_clean.Orders`
  GROUP BY customer_id
)

SELECT 
  CASE 
    WHEN order_count = 1 THEN 'New'
    ELSE 'Repeat'
  END AS customer_type,
  ROUND(SUM(total_spent), 2) AS revenue
FROM customer_orders
GROUP BY customer_type;

-- Product Name + Category Insights
SELECT 
  p.product_name,
  p.category,
  SUM(o.order_value) AS revenue
FROM `trans-serenity-478906-p9.ecommerce_clean.Orders` o
JOIN `trans-serenity-478906-p9.ecommerce_clean.Products` p
ON o.product_id = p.product_id
GROUP BY p.product_name, p.category
ORDER BY revenue DESC;