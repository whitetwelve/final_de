DATABASE_N_SCHEMA = "northwind.public"

DAILY_GROSS_REVENUE = \
f"""CREATE OR REPLACE TABLE {DATABASE_N_SCHEMA}.daily_gross_revenue AS
WITH gross_revenue_per_order AS (
    SELECT order_id, sum(unit_price * quantity * (1 - discount)) total_price
    FROM {DATABASE_N_SCHEMA}.order_details GROUP BY order_id
)
SELECT o.order_date, SUM(g.total_price) daily_gross_revenue FROM {DATABASE_N_SCHEMA}.orders o
JOIN gross_revenue_per_order g ON o.order_id = g.order_id
GROUP BY o.order_date
ORDER BY o.order_date;
"""

MONTLHY_GROSS_REVENUE_PER_PRODUCT = \
f"""CREATE OR REPLACE TABLE {DATABASE_N_SCHEMA}.montlhy_gross_revenue_per_product AS
WITH gross_revenue_per_order_product AS (
    SELECT
        p.product_name, od.order_id,
        SUM(od.unit_price * od.quantity * (1 - od.discount)) total_price
    FROM {DATABASE_N_SCHEMA}.order_details od
    JOIN {DATABASE_N_SCHEMA}.products p on od.product_id = p.product_id
    GROUP BY p.product_name, od.order_id
)
SELECT
    g.product_name,
    DATE_TRUNC('MONTH', o.order_date) month,
    SUM(g.total_price) monthly_gross_revenue
FROM {DATABASE_N_SCHEMA}.orders o
JOIN gross_revenue_per_order_product g ON o.order_id = g.order_id
GROUP BY month, g.product_name
ORDER BY month asc, monthly_gross_revenue desc;
"""

MONTHLY_QTY_SALES_PER_PRODUCT = \
f"""CREATE OR REPLACE TABLE {DATABASE_N_SCHEMA}.monthly_qty_sales_per_product AS
WITH total_sales_per_order_product AS (
    SELECT p.product_name, od.order_id, SUM(od.quantity) quantity_sold
    FROM {DATABASE_N_SCHEMA}.order_details od
    JOIN {DATABASE_N_SCHEMA}.products p on od.product_id = p.product_id
    GROUP BY p.product_name, od.order_id
)
SELECT t.product_name, DATE_TRUNC('MONTH', o.order_date) month, SUM(t.quantity_sold) quantity_sold
FROM {DATABASE_N_SCHEMA}.orders o
JOIN total_sales_per_order_product t ON o.order_id = t.order_id
GROUP BY month, t.product_name
ORDER BY month asc, quantity_sold desc
"""

MONTHLY_QTY_SALES_PER_CATEGORY = \
f"""CREATE OR REPLACE TABLE {DATABASE_N_SCHEMA}.monthly_qty_sales_per_category AS
WITH gross_revenue_per_category AS (
    SELECT p.category_id, od.order_id, SUM(od.quantity) quantity_sold
    FROM {DATABASE_N_SCHEMA}.order_details od
    JOIN {DATABASE_N_SCHEMA}.products p on od.product_id = p.product_id
    GROUP BY p.category_id, od.order_id
)
SELECT c.category_name, DATE_TRUNC('MONTH', o.order_date) month, SUM(g.quantity_sold) quantity_sold
FROM gross_revenue_per_category g
JOIN {DATABASE_N_SCHEMA}.orders o ON g.order_id = o.order_id
JOIN {DATABASE_N_SCHEMA}.categories c ON g.category_id = c.category_id
GROUP BY month, c.category_name
ORDER BY month asc, quantity_sold desc
"""

MONTHLY_TOTAL_ORDERS_PER_COUNTRY = \
f"""CREATE OR REPLACE TABLE {DATABASE_N_SCHEMA}.montlhy_total_orders_per_country AS
SELECT ship_country, DATE_TRUNC('MONTH', order_date) month, count(1) total_orders
FROM {DATABASE_N_SCHEMA}.orders
GROUP BY month, ship_country
ORDER BY month asc, total_orders desc
"""