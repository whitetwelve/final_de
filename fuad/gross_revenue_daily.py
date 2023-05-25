import snowflake.connector

# Connect to Snowflake


snowflake_cursor = snowflake_connection.cursor()

#ctas gross rev daily 
gross_revenue_daily = """
CREATE OR REPLACE TABLE GROSS_REVENUE_DAILY AS 
SELECT DATE(ORDER_DATE) AS ORDER_DATE,
    SUM((UNIT_PRICE * QUANTITY) - (UNIT_PRICE * QUANTITY * DISCOUNT)) AS GROSS_REVENUE_DAILY
FROM PUBLIC.GROSS_REVENUE
GROUP BY DATE(ORDER_DATE)
ORDER BY DATE(ORDER_DATE);
"""

snowflake_cursor.execute(gross_revenue_daily)
snowflake_connection.commit()

snowflake_cursor.close()
snowflake_connection.close()