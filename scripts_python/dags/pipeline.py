from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from src import csv_to_mysql_script
from src import ETL_process
from src import dispersion_postgres


default_args = {
    'owner': 'arkon_data',
    'start_date': datetime(2020, 4, 25),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_conekta',
    default_args=default_args,
    catchup=False,
    description='This task will extract data from csv file and save on a mysql table',
    schedule_interval="@once",
)


load_data = PythonOperator(
    task_id="load_data",
    python_callable=csv_to_mysql_script.loadData,
    dag=dag
)

etl_data_raw = PythonOperator(
    task_id="etl_data_raw",
    python_callable=ETL_process.applyETL_MysqlToParquet,
    dag=dag
)



etl_parquet_postgres = PythonOperator(
    task_id="etl_parquet_postgres",
    python_callable=dispersion_postgres.applyEtlParquetToPostgres,
    dag=dag
)



load_data >> etl_data_raw >> etl_parquet_postgres