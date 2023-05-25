from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

my_arg = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='dag_fuad', 
    default_args=my_arg,
    schedule_interval='30 1 * * *',
    catchup=False,
    dagrun_timeout=timedelta(minutes=60)
)

# Ingest data from postgres
data_ingestion = BashOperator(
    task_id='data_ingestion',
    bash_command='python /root/airflow/dags/script_fuad/ingestion.py',
    dag=dag
)

#ctas gross rev
gross_rev = BashOperator(
    task_id='gross_rev',
    bash_command='python /root/airflow/dags/script_fuad/gross_revenue.py',
    dag=dag
)

# ctas daily gross rev 
gross_rev_daily = BashOperator(
    task_id='gross_rev_daily',
    bash_command='python /root/airflow/dags/script_fuad/gross_revenue_daily.py',
    dag=dag
)

#ctas gross rev monthly 
gross_rev_monthly = BashOperator(
    task_id='gross_rev_monthly',
    bash_command='python /root/airflow/dags/script_fuad/gross_rev_per_product_monthly.py',
    dag=dag
)

#ctas purchases product monthly 
purchases_product_monthly = BashOperator(
    task_id='purchases_product_monthly',
    bash_command='python /root/airflow/dags/script_fuad/purchases_product_monthly.py',
    dag=dag
)

#ctas purchases cat-product monthly 
purchases_catprod_monthly = BashOperator(
    task_id='purchases_catprod_monthly',
    bash_command='python /root/airflow/dags/script_fuad/purchases_catepro_monthly.py',
    dag=dag
)

#ctas purchases country monthly 
purchases_country_monthly = BashOperator(
    task_id='purchases_country_monthly',
    bash_command='python /root/airflow/dags/script_fuad/purchases_countries_monthly.py',
    dag=dag
)


# Set dependency between tasks
data_ingestion >> gross_rev >> gross_rev_daily >> gross_rev_monthly >> purchases_product_monthly >> purchases_catprod_monthly >> purchases_country_monthly
