from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Görev olarak çalışacak Python fonksiyonları
def say_hello1():
    logging.info("test OP Airflow!")



# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=605),
}

# DAG tanımı
with DAG(
    'testdag',
    default_args=default_args,
    description='Airflow 3.0.1 ile uyumlu örnek DAG',
    start_date=datetime(2025, 8, 6),
    catchup=False,
    tags=['example'],
) as dag:

    start = PythonOperator(
        task_id='test_op',
        python_callable=say_hello1,
    )


    start
