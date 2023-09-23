from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from exercise_days_13.process import Extract, Transform, Load
import pendulum
import os

CWD = os.getcwd()
DATA_PATH = os.path.join(CWD, 'dags/exercise_days_13/data')

with DAG(
    'exercise_days_13',
    schedule_interval=None,
    catchup=False,
    default_args={
        'owner': 'muhidin',
        'start_date': pendulum.datetime(2023, 9, 1, tz="Asia/Jakarta"),
        'retries' : 0
    }
) as dag:

    def extract_func():
        extract = Extract(DATA_PATH)
        extract.extract_processing()
    
    def transform_func():
        transform = Transform(DATA_PATH)
        transform.transform_processing()

    def load_func():
        load = Load(DATA_PATH)
        load.load_procesing()

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_func
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_func
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_func
    )

    extract_task >> transform_task >> load_task