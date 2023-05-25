import snowflake.connector

# Connect to Snowflake

snowflake_cursor = snowflake_connection.cursor()

#ctas purchases country monthly
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


snowflake_cursor.close()
snowflake_connection.close()