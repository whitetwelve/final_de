from packages.PgToSfDataIngester import PgToSfDataIngester
from connparams.ConnectionParams import PG_CONN_PARAMS
from connparams.ConnectionParams import SF_CONN_PARAMS
import packages.SfCreateDatamartSQL as SfMartSQL
import psycopg2
import snowflake.connector
import csv
import os

# SETUP DATABASE CONNECTION AND PYTHON TASKS
PG_DB_NAME = "defaultdb"
SF_DB_NAME = "NORTHWIND"
PG_SCHEMA = "public"
SF_SCHEMA = "PUBLIC"
CSV_STG = (
    "/home/dea_chintiaputri/products_stg.csv",
    "/home/dea_chintiaputri/orders_stg.csv",
    "/home/dea_chintiaputri/order_details_stg.csv",
    "/home/dea_chintiaputri/categories_stg.csv",
)

TableToIngest = (
    (PG_DB_NAME, "products", SF_DB_NAME, "PRODUCTS", PG_SCHEMA, SF_SCHEMA, CSV_STG[0]),
    (PG_DB_NAME, "orders", SF_DB_NAME, "ORDERS", PG_SCHEMA, SF_SCHEMA, CSV_STG[1]),
    (PG_DB_NAME, "order_details", SF_DB_NAME, "ORDER_DETAILS", PG_SCHEMA, SF_SCHEMA, CSV_STG[2]),
    (PG_DB_NAME, "categories", SF_DB_NAME, "CATEGORIES", PG_SCHEMA, SF_SCHEMA, CSV_STG[3]),
)

def ingestProductsData():
    pg_conn = psycopg2.connect(**PG_CONN_PARAMS)
    pg_cursor = pg_conn.cursor()
    sf_conn = snowflake.connector.connect(**SF_CONN_PARAMS)
    sf_cursor = sf_conn.cursor()

    products_ingester = PgToSfDataIngester(pg_cursor, sf_cursor,*TableToIngest[0])
    products_ingester.autoIngest()
    
    pg_cursor.close()
    pg_conn.close()
    sf_cursor.close()
    sf_conn.close()


def ingestOrdersData():
    pg_conn = psycopg2.connect(**PG_CONN_PARAMS)
    pg_cursor = pg_conn.cursor()
    sf_conn = snowflake.connector.connect(**SF_CONN_PARAMS)
    sf_cursor = sf_conn.cursor()
    
    orders_ingester = PgToSfDataIngester(pg_cursor, sf_cursor,*TableToIngest[1])
    orders_ingester.autoIngest()
    
    pg_cursor.close()
    pg_conn.close()
    sf_cursor.close()
    sf_conn.close()

def ingestOrderDetailsData():
    pg_conn = psycopg2.connect(**PG_CONN_PARAMS)
    pg_cursor = pg_conn.cursor()
    sf_conn = snowflake.connector.connect(**SF_CONN_PARAMS)
    sf_cursor = sf_conn.cursor()
    
    order_details_ingester = PgToSfDataIngester(pg_cursor, sf_cursor,*TableToIngest[2])
    order_details_ingester.autoIngest()
    
    pg_cursor.close()
    pg_conn.close()
    sf_cursor.close()
    sf_conn.close()

def ingestCategoriesData():
    pg_conn = psycopg2.connect(**PG_CONN_PARAMS)
    pg_cursor = pg_conn.cursor()
    sf_conn = snowflake.connector.connect(**SF_CONN_PARAMS)
    sf_cursor = sf_conn.cursor()
    
    categories_ingester = PgToSfDataIngester(pg_cursor, sf_cursor,*TableToIngest[3])
    categories_ingester.autoIngest()
    
    pg_cursor.close()
    pg_conn.close()
    sf_cursor.close()
    sf_conn.close()

def createDailyGrossRevenueDm():
    sf_conn = snowflake.connector.connect(**SF_CONN_PARAMS)
    sf_cursor = sf_conn.cursor()
    
    sf_cursor.execute(SfMartSQL.DAILY_GROSS_REVENUE)
    
    sf_cursor.close()
    sf_conn.close()

def createMontlhyGrossRevenuePerProductDm():
    sf_conn = snowflake.connector.connect(**SF_CONN_PARAMS)
    sf_cursor = sf_conn.cursor()
    
    sf_cursor.execute(SfMartSQL.MONTLHY_GROSS_REVENUE_PER_PRODUCT)
    
    sf_cursor.close()
    sf_conn.close()

def createMonthlyQtySalesPerProductDm():
    sf_conn = snowflake.connector.connect(**SF_CONN_PARAMS)
    sf_cursor = sf_conn.cursor()
    
    sf_cursor.execute(SfMartSQL.MONTHLY_QTY_SALES_PER_PRODUCT)
    
    sf_cursor.close()
    sf_conn.close()

def createMonthlyQtySalesPerCategoryDm():
    sf_conn = snowflake.connector.connect(**SF_CONN_PARAMS)
    sf_cursor = sf_conn.cursor()
    
    sf_cursor.execute(SfMartSQL.MONTHLY_QTY_SALES_PER_CATEGORY)
    
    sf_cursor.close()
    sf_conn.close()

def createMonthlyTotalOrdersPerCountryDm():
    sf_conn = snowflake.connector.connect(**SF_CONN_PARAMS)
    sf_cursor = sf_conn.cursor()
    
    sf_cursor.execute(SfMartSQL.MONTHLY_TOTAL_ORDERS_PER_COUNTRY)
    
    sf_cursor.close()
    sf_conn.close()