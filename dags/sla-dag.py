from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import time


def my_custom_function(ts,**kwargs):
    print("task is sleeping")
    # 0 / 0
    time.sleep(40)


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': 'noreply@astronomer.io',
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(seconds=30)
}

# Using a DAG context manager, you don't have to specify the dag property of each task
with DAG('sla-dag',
         start_date=datetime(2021, 1, 1),
         max_active_runs=1,
         schedule_interval=timedelta(minutes=2),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False 
         ) as dag:

    t0 = DummyOperator(
        task_id='start',
        # sla=timedelta(seconds=50)
    )

    t1 = DummyOperator(
        task_id='end',
        # sla=timedelta(seconds=500)
    )

    sla_task = PythonOperator(
        task_id='sla_task',
        python_callable=my_custom_function,
        # sla=timedelta(seconds=5)
    )

    t0 >> sla_task >> t1