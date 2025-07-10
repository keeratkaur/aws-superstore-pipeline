-- Total sales by category
SELECT category, SUM(sales) AS total_sales
FROM superstore_orders
GROUP BY category
ORDER BY total_sales DESC;

-- Number of records by city
SELECT city, COUNT(*) AS total_orders
FROM superstore_orders
GROUP BY city
ORDER BY total_orders DESC;
