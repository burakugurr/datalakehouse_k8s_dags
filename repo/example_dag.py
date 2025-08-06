from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

# Görev olarak çalışacak Python fonksiyonu
def say_hello1():
    logging.info("Merhaba, Airflow!")
# Görev olarak çalışacak Python fonksiyonu
def say_hello2():
    logging.info("runing!")


# Görev olarak çalışacak Python fonksiyonu
def say_hello3():
    print("goodby")
# DAG varsayılan argümanlar
# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# DAG tanımı
with DAG(
    'example_airflow_3',
    default_args=default_args,
    description='Airflow 3.0.1 ile uyumlu örnek DAG',
    start_date=datetime(2025, 8, 6),
    catchup=False,
    tags=['example'],
) as dag:

    start =  PythonOperator(
        task_id='print_hello1',
        python_callable=say_hello1
    )

    hello_task = PythonOperator(
        task_id='print_hello2',
        python_callable=say_hello2
    )

    end =  PythonOperator(
        task_id='print_goodby3',
        python_callable=say_hello3
    )

    # Görevler arası sıra
    start >> hello_task >> end
