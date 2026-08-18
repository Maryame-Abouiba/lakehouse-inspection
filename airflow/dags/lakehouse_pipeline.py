from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import docker

def run_spark_job(script_name):
    """Exécute un script PySpark à l'intérieur du conteneur spark-iceberg déjà en cours d'exécution."""
    client = docker.from_env()
    container = client.containers.get("spark-iceberg")
    exit_code, output = container.exec_run(f"spark-submit /home/iceberg/jobs/{script_name}")
    log_output = output.decode("utf-8")
    print(log_output)
    if exit_code != 0:
        raise Exception(f"❌ Échec de {script_name} (code {exit_code})\n{log_output}")
    print(f"✅ {script_name} terminé avec succès")

default_args = {
    "owner": "stage-si",
    "retries": 0,
}

with DAG(
    dag_id="lakehouse_inspection_pipeline",
    default_args=default_args,
    description="Pipeline Medallion : Bronze -> Silver -> Gold pour le use case inspection bâtiment",
    schedule=None,  # déclenchement manuel uniquement pour l'instant
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lakehouse", "inspection"],
) as dag:

    bronze = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_spark_job,
        op_kwargs={"script_name": "01_bronze_ingestion.py"},
    )

    silver = PythonOperator(
        task_id="silver_transform",
        python_callable=run_spark_job,
        op_kwargs={"script_name": "02_silver_transform.py"},
    )

    gold = PythonOperator(
        task_id="gold_aggregation",
        python_callable=run_spark_job,
        op_kwargs={"script_name": "03_gold_aggregation.py"},
    )

    bronze >> silver >> gold