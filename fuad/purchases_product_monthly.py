import snowflake.connector

# Connect to Snowflake

snowflake_cursor = snowflake_connection.cursor()

#ctas purchases product monthly
purchases_products_monthly= """
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

snowflake_cursor.close()
snowflake_connection.close()