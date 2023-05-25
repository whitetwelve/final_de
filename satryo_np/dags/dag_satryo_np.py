from __future__ import annotations
import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="dag_satryo",
    default_args={
        "owner": "Satryo Nugroho Pryambodo",
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=2),
    },
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=10),
) as dag:
    task1 = EmptyOperator(
        task_id='data_ingestion_with_airbytes_platform'
    )
    task2 = BashOperator(
        task_id='data_mart_load',
        bash_command='python3 /home/n_p_satryo/airflow/scripts/update.py'
    )

    task1 >> task2

if __name__ == "__main__":
    dag.test()
