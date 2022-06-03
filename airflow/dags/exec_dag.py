from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy_operator import DummyOperator
import os

HOST_WORKING_DIRECTORY = os.environ["HOST_WORKING_DIRECTORY"]


default_args = {
    "owner": "airflow",
    "description": "Use of the DockerOperator for gitlab exercise",
    "depend_on_past": False,
    "start_date": datetime(2021, 5, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "extract_tasks",
    default_args=default_args,
    schedule_interval="5 * * * *",
    catchup=False,
) as dag:
    start_dag = DummyOperator(task_id="start_dag")

    end_dag = DummyOperator(task_id="end_dag")

    extraction_metrics = DockerOperator(
        task_id="run_extract",
        image="jupyter/all-spark-notebook:latest",
        container_name="dev-template_spark",
        api_version="1.21",
        auto_remove=True,
        working_dir="/project",
        volumes=[f"{HOST_WORKING_DIRECTORY}:/project"],
        command="src/metric_spark.py",
        docker_url="tcp://docker-proxy:1234",
        network_mode="host",
    )

    extraction_grafics = DockerOperator(
        task_id="run_extract_products",
        image="jupyter/all-spark-notebook:latest",
        container_name="dev-template_grafics",
        api_version="1.21",
        auto_remove=True,
        working_dir="/project",
        volumes=[f"{HOST_WORKING_DIRECTORY}:/project"],
        command="src/grafics.py",
        docker_url="tcp://docker-proxy:1234",
        network_mode="host",
    )

    
    

    

    (
        start_dag >> extraction_metrics >>  extraction_grafics     >> end_dag
    )
