from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
import pendulum

import packages.NorthwindTaskFunctions as ntf

# DAG CONFIGURATION

with DAG(
    dag_id="dag_northwind_dea",
    default_args={
        "owner": "Dea Chintia Putri",
        "retries": 3,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    description="DAG for ingesting data from PostgreSQL to Snowflake",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2023, 5, 20, tz="UTC"),
    dagrun_timeout=datetime.timedelta(minutes=120),
    catchup=False,
) as dag:

    # Task t1 to t4 are for ingesting data from PostgreSQl to Snowflake
    t1 = PythonOperator(
        task_id="ingest_products_data",
        python_callable=ntf.ingestProductsData,
    )

    t2 = PythonOperator(
        task_id="ingest_orders_data",
        python_callable=ntf.ingestOrdersData,
    )

    t3 = PythonOperator(
        task_id="ingest_order_details_data",
        python_callable=ntf.ingestOrderDetailsData,
    )

    t4 = PythonOperator(
        task_id="ingest_categories_data",
        python_callable=ntf.ingestCategoriesData,
    )

    # Task t5 to t9 are for creating datamart views in Snowflake

    t5 = PythonOperator(
        task_id="create_daily_gross_revenue_dm",
        python_callable=ntf.createDailyGrossRevenueDm,
    )

    t6 = PythonOperator(
        task_id="create_montlhy_gross_revenue_per_product_dm",
        python_callable=ntf.createMontlhyGrossRevenuePerProductDm,
    )

    t7 = PythonOperator(
        task_id="create_monthly_qty_sales_per_product_dm",
        python_callable=ntf.createMonthlyQtySalesPerProductDm,
    )

    t8 = PythonOperator(
        task_id="create_monthly_qty_sales_per_category_dm",
        python_callable=ntf.createMonthlyQtySalesPerCategoryDm,
    )

    t9 = PythonOperator(
        task_id="create_montlhy_total_orders_per_country_dm",
        python_callable=ntf.createMonthlyTotalOrdersPerCountryDm,
    )

    # Dependencies breakdown
    # [t2] >> t9
    # [t2, t3] >> t5
    # [t1, t2, t3] >> t6
    # [t1, t2, t3] >> t7
    # [t1, t2, t3, t4] >> t8

    # The graph looked too complex
    # t2 >> [t9, t3] 
    # [t2, t3] >> t5
    # [t2, t3] >> t1 >> t4 >> t8
    # t1 >> [t6, t7]

    # This one has better graph
    t2 >> [t9, t3] 
    t3 >> [t1, t5]
    t1 >> [t4, t6, t7]
    t4 >> t8
