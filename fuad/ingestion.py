import snowflake.connector
import psycopg2

#conf pg

# conf sf


#connection for both of them
pg_cursor = pg_connection.cursor()
snowflake_cursor = snowflake_connection.cursor()

#ingest data to orders
ingest_orders = f"""
    insert into orders (order_id,customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country)
    with max_id as (select max(order_id) + 1 as order_id from orders)
    select max_id.order_id, customer_id, employee_id, current_date, current_date + interval '1 month', current_date + interval '10 days', ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country 
    from orders, max_id
    ORDER BY random()
    LIMIT 1;
"""
snowflake_cursor.execute(ingest_orders)
snowflake_connection.commit()

#ingest data or order_details
ingest_order_details = f"""
    insert into order_details (order_id, product_id, unit_price, quantity, discount)
    with max_id as (select max(order_id) as order_id from orders)
    SELECT max_id.order_id, product_id, unit_price, quantity, discount 
    FROM order_details od, max_id 
    WHERE od.order_id = max_id.order_id
    LIMIT 1;
"""
snowflake_cursor.execute(ingest_order_details)
snowflake_connection.commit()

print("Insert data success!")