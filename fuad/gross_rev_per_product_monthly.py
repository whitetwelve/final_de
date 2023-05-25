import snowflake.connector

# Connect to Snowflake


snowflake_cursor = snowflake_connection.cursor()

#ctas monthly gross rev per product
gross_revenue_monthly = """
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

snowflake_cursor.close()
snowflake_connection.close()