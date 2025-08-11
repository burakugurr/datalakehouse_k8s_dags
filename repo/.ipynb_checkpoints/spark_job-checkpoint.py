from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 10),
    "retries": 0
}

# PVC ve mount tanımı
volume = k8s.V1Volume(
    name="jars-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="jars-pvc")
)

volume_mount = k8s.V1VolumeMount(
    name="jars-volume",
    mount_path="/home/jovyan/work/jars"
)


with DAG(
    dag_id="spark_job_k8s_jupyter",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark", "k8s"]
) as dag:

    spark_job = KubernetesPodOperator(
        task_id="run_spark_job",
        name="spark-job",
        namespace="default", 
        image="jupyter/pyspark-notebook:x86_64-spark-3.5.0",
        cmds=["/bin/bash", "-c"],
        arguments=[
            "spark-submit "
            "--master spark://jupyter-spark-driver-headless.default.svc.cluster.local:7077 "
            "--deploy-mode client "
            "/opt/airflow/dags/repo/create_dag.py"  # py dosyanın doğru path’i burada
        ],
        get_logs=True,
        is_delete_operator_pod=True
    )

    spark_job
