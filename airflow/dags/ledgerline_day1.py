from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

DBT_IMAGE = "ledgerline-dbt:1.8.2"
SODA_IMAGE = "ledgerline-soda:3.3.17"

default_args = {"owner": "ledgerline"}

with DAG(
    dag_id="ledgerline_day1",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    dbt_run = DockerOperator(
        task_id="dbt_run",
        image=DBT_IMAGE,
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="ledgerline",
        working_dir="/usr/app",
        volumes=[
            # mount dbt project and profiles into the dbt container
            "./dbt:/usr/app",
        ],
        environment={
            "DBT_PROFILES_DIR": "/usr/app",
        },
        command="dbt run --project-dir /usr/app/ledgerline --profiles-dir /usr/app",
    )

    dbt_test = DockerOperator(
        task_id="dbt_test",
        image=DBT_IMAGE,
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="ledgerline",
        working_dir="/usr/app",
        volumes=["./dbt:/usr/app"],
        environment={"DBT_PROFILES_DIR": "/usr/app"},
        command="dbt test --project-dir /usr/app/ledgerline --profiles-dir /usr/app",
    )

    soda_scan = DockerOperator(
        task_id="soda_scan",
        image=SODA_IMAGE,
        api_version="auto",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="ledgerline",
        working_dir="/soda",
        volumes=[
            "./soda:/soda",
        ],
        command="soda scan -d warehouse -c /soda/configuration.yml /soda/checks.yml",
    )

    dbt_run >> dbt_test >> soda_scan