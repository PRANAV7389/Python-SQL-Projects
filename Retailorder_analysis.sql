SELECT * FROM pranav.df_orders;

-- find top 10 highest reveue generating products 
SELECT  product_id,sum(sale_price) as sales
FROM df_orders
group by product_id
order by sales desc
LIMIT 10

-- find top 5 highest selling products in each region
with cte as (
SELECT  region,product_id,sum(sale_price) as sales
FROM df_orders
group by region,product_id)
SELECT * from (
SELECT * 
,row_number() over(partition by region order by sales desc) as rn
from cte ) A
where rn<=5

-- find month over month growth comparison for 2022 and 2023 sales eg : jan 2022 vs jan 2023
with cte as (
  select year(order_date) as year, month(order_date) as month, sum(sale_price) as sales
  from df_orders
  group by year(order_date), month(order_date)
)
select month,
       sum(case when year = 2022 then sales else 0 end) as sales_2022,
       sum(case when year = 2023 then sales else 0 end) as sales_2023
from cte
group by month
order by month;

-- for each category which month had highest sales 
with cte as (
select category,format(order_date,'yyyyMM') AS order_year_month, sum(sale_price) as sales
from df_orders
group by category,format(order_date,'yyyyMM')
)
select * from (
select *,
row_number() over(partition by category order by sales desc) as rn
from cte
) a
where rn=1

-- which sub category had highest growth by profit in 2023 compare to 2022
with cte as (
  select sub_category, year(order_date) as year, sum(sale_price) as sales
  from df_orders
  group by sub_category,year(order_date)
)
, cte2 as (
select sub_category,
       sum(case when year = 2022 then sales else 0 end) as sales_2022,
       sum(case when year = 2023 then sales else 0 end) as sales_2023
from cte
group by sub_category
)

select *,
(sales_2023-sales_2022)*100/sales_2022
from cte2
order by (sales_2023-sales_2022)*100/sales_2022 desc
LIMIT 1
