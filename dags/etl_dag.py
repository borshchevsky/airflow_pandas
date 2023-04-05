from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pyspark.sql import SparkSession

from etl.get_posts import get_posts
from etl.get_todos import get_todos
from etl.get_users import get_users
from etl.load import load

spark = SparkSession.builder.appName('app').getOrCreate()

with DAG(
        dag_id='ETL',
        description='ETL',
        # schedule_interval='*/1 * * * *',
        schedule=None,
        catchup=False,
        start_date=datetime(2023, 4, 3),
        max_active_runs=1
) as dag:
    t1 = PythonOperator(
        dag=dag,
        task_id='Users',
        python_callable=get_users,
        execution_timeout=timedelta(minutes=5),
        op_kwargs={'session': spark}
    )

    t2 = PythonOperator(
        dag=dag,
        task_id='Posts',
        python_callable=get_posts,
        execution_timeout=timedelta(minutes=5),
        op_kwargs={'session': spark}
    )

    t3 = PythonOperator(
        dag=dag,
        task_id='Todos',
        python_callable=get_todos,
        execution_timeout=timedelta(minutes=5),
        op_kwargs={'session': spark}
    )

    t4 = PythonOperator(
        dag=dag,
        task_id='UserInfo',
        python_callable=load,
        execution_timeout=timedelta(minutes=5),
        op_kwargs={'session': spark}
    )

    t1 >> t2 >> t4
    t1 >> t3 >> t4
