from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

# Görev olarak çalışacak Python fonksiyonu
def say_hello():
    print("Merhaba, Airflow!")

# DAG varsayılan argümanlar
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
    'ornek_dag',
    default_args=default_args,
    description='Basit bir örnek DAG',
    schedule_interval='@daily',  # Her gün çalışır
    start_date=datetime(2025, 8, 1),
    catchup=False,  # Eski tarihli işleri çalıştırma
    tags=['ornek', 'tutorial'],
) as dag:

    start = DummyOperator(task_id='start')

    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=say_hello,
    )

    end = DummyOperator(task_id='end')

    # Görevler arası sıra
    start >> hello_task >> end
