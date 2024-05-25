-- Select all records from the df_orders table
SELECT * FROM pranav.df_orders;

-- Find top 10 highest revenue-generating products
-- 1. Group the records by product_id
-- 2. Calculate the sum of sale_price for each product_id
-- 3. Order the results by sales in descending order
-- 4. Limit the results to the top 10 entries
SELECT product_id, sum(sale_price) as sales
FROM df_orders
GROUP BY product_id
ORDER BY sales DESC
LIMIT 10;

-- Find top 5 highest selling products in each region
-- 1. Create a common table expression (CTE) to calculate sales per product_id for each region
-- 2. Use ROW_NUMBER() to rank the products within each region by sales in descending order
-- 3. Select the top 5 products (rn <= 5) for each region
WITH cte AS (
  SELECT region, product_id, sum(sale_price) as sales
  FROM df_orders
  GROUP BY region, product_id
)
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY region ORDER BY sales DESC) as rn
  FROM cte
) A
WHERE rn <= 5;

-- Find month-over-month growth comparison for 2022 and 2023 sales
-- 1. Create a CTE to calculate the total sales per month for each year
-- 2. Use CASE statements to segregate sales data for 2022 and 2023
-- 3. Group the results by month and order by month
WITH cte AS (
  SELECT YEAR(order_date) as year, MONTH(order_date) as month, sum(sale_price) as sales
  FROM df_orders
  GROUP BY YEAR(order_date), MONTH(order_date)
)
SELECT month,
  sum(CASE WHEN year = 2022 THEN sales ELSE 0 END) as sales_2022,
  sum(CASE WHEN year = 2023 THEN sales ELSE 0 END) as sales_2023
FROM cte
GROUP BY month
ORDER BY month;

-- For each category, find the month with the highest sales
-- 1. Create a CTE to calculate sales per category for each year-month
-- 2. Use ROW_NUMBER() to rank the months within each category by sales in descending order
-- 3. Select the month with the highest sales (rn = 1) for each category
WITH cte AS (
  SELECT category, FORMAT(order_date, 'yyyyMM') AS order_year_month, sum(sale_price) as sales
  FROM df_orders
  GROUP BY category, FORMAT(order_date, 'yyyyMM')
)
SELECT * FROM (
  SELECT *,
    ROW_NUMBER() OVER(PARTITION BY category ORDER BY sales DESC) as rn
  FROM cte
) a
WHERE rn = 1;

-- Identify which sub-category had the highest growth by profit in 2023 compared to 2022
-- 1. Create a CTE to calculate sales per sub_category for each year
-- 2. Create another CTE to calculate the growth by profit between 2023 and 2022 for each sub_category
-- 3. Order the results by growth in descending order and limit to the top 1 entry
WITH cte AS (
  SELECT sub_category, YEAR(order_date) as year, sum(sale_price) as sales
  FROM df_orders
  GROUP BY sub_category, YEAR(order_date)
), cte2 AS (
  SELECT sub_category,
    sum(CASE WHEN year = 2022 THEN sales ELSE 0 END) as sales_2022,
    sum(CASE WHEN year = 2023 THEN sales ELSE 0 END) as sales_2023
  FROM cte
  GROUP BY sub_category
)
SELECT *,
  (sales_2023 - sales_2022) * 100 / sales_2022 as growth_percentage
FROM cte2
ORDER BY growth_percentage DESC
LIMIT 1;
