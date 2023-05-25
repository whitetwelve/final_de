import snowflake.connector

# Connect to Snowflake


snowflake_cursor = snowflake_connection.cursor()

#ctas gross revenue
gross_revenue= """
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

snowflake_cursor.close()
snowflake_connection.close()