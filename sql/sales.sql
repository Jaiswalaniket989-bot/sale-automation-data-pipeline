CREATE TABLE sales (
    order_id INT,
    product_id INT,
    category TEXT,
    price FLOAT,
    cost FLOAT,
    quantity INT,
    order_date DATE,
    region TEXT,
    discount FLOAT,
    competitor_price FLOAT,
    revenue FLOAT,
    profit FLOAT
);

select * from sales;

--1.Total Revenue
SELECT *
FROM sales
ORDER BY order_date DESC
LIMIT 10;

--2.Total Profit
SELECT SUM((price - cost) * quantity) AS total_profit
FROM sales;

--3.Profit Margin (%)
SELECT 
    (SUM((price - cost) * quantity) * 100.0 / SUM(price * quantity))::numeric(10,2) 
    AS profit_margin
FROM sales;

--4.Total Orders
SELECT COUNT(DISTINCT order_id) AS total_orders
FROM sales;

--5.Revenue by Category
SELECT category, SUM(price * quantity) AS revenue
FROM sales
GROUP BY category
ORDER BY revenue DESC;

--6.Profit by region
SELECT region, SUM((price - cost) * quantity) AS profit
FROM sales
GROUP BY region
ORDER BY profit DESC;

--7.Monthly sales Trend
SELECT 
    DATE_TRUNC('month', order_date) AS month,
    SUM(price * quantity) AS revenue
FROM sales
GROUP BY month
ORDER BY month;

--8.Top 5 Products by Revenue
SELECT product_id, SUM(price * quantity) AS revenue
FROM sales
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 5;

--9.Duplicates orders
SELECT order_id, COUNT(*)
FROM sales
GROUP BY order_id
HAVING COUNT(*) > 1;

--10.Latest data check
SELECT MAX(order_date) AS latest_date
FROM sales;

--11.Rows Insterted Recently 
SELECT * 
FROM sales
ORDER BY order_date DESC
LIMIT 20;

--12.TOP Products 
SELECT 
    product_id,
    SUM(price * quantity) AS revenue,
    RANK() OVER (ORDER BY SUM(price * quantity) DESC) AS rank
FROM sales
GROUP BY product_id;

--13.Running Total
SELECT 
    order_date,
    SUM(price * quantity) AS daily_revenue,
    SUM(SUM(price * quantity)) OVER (ORDER BY order_date) AS running_total
FROM sales
GROUP BY order_date
ORDER BY order_date;

--14.Revenue contibution %
SELECT 
    category,
    (SUM(price * quantity) * 100.0 / SUM(SUM(price * quantity)) OVER ())::numeric(10,2) AS contribution_percent
FROM sales
GROUP BY category;

--15.Month-over-month Growth
SELECT 
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY month) AS prev_month,
    (revenue - LAG(revenue) OVER (ORDER BY month)) AS growth
FROM (
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        SUM(price * quantity) AS revenue
    FROM sales
    GROUP BY month
) t;

--16.Customer Cohort analysis
SELECT 
    DATE_TRUNC('month', order_date) AS cohort_month,
    COUNT(DISTINCT order_id) AS total_orders
FROM sales
GROUP BY cohort_month
ORDER BY cohort_month;


