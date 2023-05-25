import snowflake.connector
import psycopg2

# config pg

# config sf


#cursor for both of them 
pg_cursor = pg_connection.cursor()
snowflake_cursor = snowflake_connection.cursor()


#ctas gross revenue
gross_revenue= f"""
CREATE OR REPLACE TABLE GROSS_REVENUE AS (
    SELECT a.quantity,
        b.PRODUCT_NAME,
        a.product_id,
        a.discount,
        a.unit_price,
        a.order_id,
        c.order_date
    FROM PUBLIC.ORDER_DETAILS a
    INNER JOIN PUBLIC.PRODUCTS b
        ON a.product_id = b.product_id
    INNER JOIN PUBLIC.ORDERS c
        ON a.order_id = c.order_id
);"""

snowflake_cursor.execute(gross_revenue)
snowflake_connection.commit()


#ctas gross rev daily 
gross_revenue_daily = """
CREATE OR REPLACE TABLE GROSS_REVENUE_DAILY AS 
SELECT DATE(ORDER_DATE) AS ORDER_DATE,
    SUM((UNIT_PRICE * QUANTITY) - (UNIT_PRICE * QUANTITY * DISCOUNT)) AS "GROSS REVENUE DAILY"
FROM PUBLIC.GROSS_REVENUE
GROUP BY DATE(ORDER_DATE)
ORDER BY DATE(ORDER_DATE);
"""

#ctas monthly gross rev per product
gross_revenue_monthly = f""""
CREATE OR REPLACE TABLE GROSS_REVENUE_MONTH AS
SELECT
    CASE
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 1 THEN 'Januari'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 2 THEN 'Februari'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 3 THEN 'Maret'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 4 THEN 'April'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 5 THEN 'Mei'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 6 THEN 'Juni'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 7 THEN 'Juli'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 8 THEN 'Agustus'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 9 THEN 'September'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 10 THEN 'Oktober'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 11 THEN 'November'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 12 THEN 'Desember'
    END AS "MONTHLY",
    PRODUCT_NAME,
    EXTRACT(MONTH FROM ORDER_DATE) AS "MONTH",
    SUM(UNIT_PRICE * QUANTITY * (1 - DISCOUNT)) AS GROSS_REVENUE
FROM
    PUBLIC.GROSS_REVENUE
GROUP BY
    PRODUCT_NAME,
    EXTRACT(MONTH FROM ORDER_DATE)
ORDER BY
    PRODUCT_NAME,
    "MONTHLY";
"""
snowflake_cursor.execute(gross_revenue_monthly)
snowflake_connection.commit()

#ctas purchases product monthly
purchases_products_monthly= f"""
CREATE OR REPLACE TABLE PURCHASES_MONTHLY_PRODUCT
AS SELECT 
    EXTRACT(MONTH FROM O.ORDER_DATE) AS Month,
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    SUM(od.QUANTITY) AS TOTAL_PURCHASES
FROM
    ORDERS o
    INNER JOIN ORDER_DETAILS od ON o.ORDER_ID = od.ORDER_ID
    INNER JOIN PRODUCTS p ON od.PRODUCT_ID = p.PRODUCT_ID
GROUP BY
    EXTRACT(MONTH FROM o.ORDER_DATE),
    p.PRODUCT_ID,
    p.PRODUCT_NAME
ORDER BY
    Month ASC,
    p.PRODUCT_ID ASC;
"""
snowflake_cursor.execute(purchases_products_monthly)
snowflake_connection.commit()

#fetch top 10 top pembelian product per bulan 
purchases10_products_monthly = f"""
    SELECT PRODUCT_ID,PRODUCT_NAME,TOTAL_PURCHASES,
CASE
        WHEN MONTH = 1 THEN 'Januari'
        WHEN MONTH = 2 THEN 'Februari'
        WHEN MONTH = 3 THEN 'Maret'
        WHEN MONTH = 4 THEN 'April'
        WHEN MONTH = 5 THEN 'Mei'
        WHEN MONTH = 6 THEN 'Juni'
        WHEN MONTH = 7 THEN 'Juli'
        WHEN MONTH = 8 THEN 'Agustus'
        WHEN MONTH = 9 THEN 'September'
        WHEN MONTH = 10 THEN 'Oktober'
        WHEN MONTH = 11 THEN 'November'
        WHEN MONTH = 12 THEN 'Desember'
    END AS "MONTHLY"
FROM PURCHASES_MONTHLY_PRODUCT
ORDER BY TOTAL_PURCHASES DESC
LIMIT 10;
"""
snowflake_cursor.execute(purchases10_products_monthly)
snowflake_connection.commit()

#purchases per category product monthly
purchases_category_product_monthly= f"""
CREATE OR REPLACE TABLE PURCHASES_CATEGORY_PRODUCT_MONTHLY AS
SELECT
        CASE
        WHEN EXTRACT(MONTH FROM o.ORDER_DATE) = 1 THEN 'Januari'
        WHEN EXTRACT(MONTH FROM o.ORDER_DATE) = 2 THEN 'Februari'
        WHEN EXTRACT(MONTH FROM o.ORDER_DATE) = 3 THEN 'Maret'
        WHEN EXTRACT(MONTH FROM o.ORDER_DATE) = 4 THEN 'April'
        WHEN EXTRACT(MONTH FROM o.ORDER_DATE) = 5 THEN 'Mei'
        WHEN EXTRACT(MONTH FROM o.ORDER_DATE) = 6 THEN 'Juni'
        WHEN EXTRACT(MONTH FROM o.ORDER_DATE) = 7 THEN 'Juli'
        WHEN EXTRACT(MONTH FROM o.ORDER_DATE) = 8 THEN 'Agustus'
        WHEN EXTRACT(MONTH FROM o.ORDER_DATE) = 9 THEN 'September'
        WHEN EXTRACT(MONTH FROM o.ORDER_DATE) = 10 THEN 'Oktober'
        WHEN EXTRACT(MONTH FROM o.ORDER_DATE) = 11 THEN 'November'
        WHEN EXTRACT(MONTH FROM o.ORDER_DATE) = 12 THEN 'Desember'
    END AS "MONTHLY",
    c.category_id,
    c.category_name,
    SUM(od.QUANTITY) AS Total_Purchases
FROM
    ORDER_DETAILS od
    INNER JOIN PRODUCTS p ON od.product_id = p.product_id
    INNER JOIN CATEGORIES c ON p.category_id = c.category_id
    INNER JOIN ORDERS o ON od.order_id = o.order_id
GROUP BY
    EXTRACT(MONTH FROM o.ORDER_DATE),
    c.category_id,
    c.category_name;
"""
snowflake_cursor.execute(purchases_category_product_monthly)
snowflake_connection.commit()

#fetch top 10 pembelian perkategori produk bulanan
purchases10_category_product_monthly = f"""
SELECT * FROM PURCHASES_CATEGORY_PRODUCT_MONTHLY
ORDER BY TOTAL_PURCHASES DESC
LIMIT 10;
"""
snowflake_cursor.execute(purchases10_category_product_monthly)
snowflake_connection.commit()

#total pembelian per negara per bulan
purchases_monthly_country = f"""
CREATE OR REPLACE TABLE PURCHASES_MONTHLY_COUNTRY AS
SELECT
    CASE
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 1 THEN 'Januari'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 2 THEN 'Februari'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 3 THEN 'Maret'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 4 THEN 'April'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 5 THEN 'Mei'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 6 THEN 'Juni'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 7 THEN 'Juli'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 8 THEN 'Agustus'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 9 THEN 'September'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 10 THEN 'Oktober'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 11 THEN 'November'
        WHEN EXTRACT(MONTH FROM ORDER_DATE) = 12 THEN 'Desember'
    END AS "MONTHLY",
    os.SHIP_COUNTRY AS COUNTRY,
    SUM(od.QUANTITY) AS TOTAL_PURCHASES
FROM 
    order_details AS od 
    INNER JOIN orders AS os
    ON od.ORDER_ID = os.ORDER_ID
GROUP BY 
    EXTRACT(MONTH FROM os.ORDER_DATE),
    os.SHIP_COUNTRY;
"""
snowflake_cursor.execute(purchases_monthly_country)
snowflake_connection.commit()


