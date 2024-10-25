import os
import sys
print(sys.path)
sys.path.append(os.path.join('/', 'home', 'ubuntu', 'airflow', "dags", "covid_dag"))

from extract import Extract
from transform import Transform
from load import Load
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import datetime as dt

default_args = {
    'retries' : 2,
    'retry_delay' : dt.timedelta(minutes=2),
    'email_on_retry' : False,
    'email_on_failure' : False,
}

dag = DAG(
    'covid_to_pg',
    default_args=default_args,
    start_date=dt.datetime(2024,10,23),
    schedule_interval='@daily',
    catchup=True
)

def run_etl(ds=None):
    #for each day, load the previous day
    extract = Extract(dt.datetime.strptime(ds,'%Y-%m-%d'))
    df = extract.execute_extraction()
    transform = Transform(df)
    transformed_df = transform.transform_data()
    load = Load(transformed_df, postgres_conn_id="citus")
    load.load()

t1 = PythonOperator(
    task_id = "run_etl",
    python_callable=run_etl,
    dag=dag
)

t2 = PostgresOperator(
    task_id="check_postgresql",
    postgres_conn_id="citus",  # PostgreSQL connection in Airflow
    sql="""
    SELECT COUNT(*)
    FROM covid_data
    """,
    dag=dag
)

t1 >> t2