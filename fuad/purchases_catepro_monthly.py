import snowflake.connector

# Connect to Snowflake

snowflake_cursor = snowflake_connection.cursor()

#ctas purchases category product monthly
purchases_catprod_monthly= """
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
snowflake_cursor.execute(purchases_catprod_monthly)
snowflake_connection.commit()


snowflake_cursor.close()
snowflake_connection.close()