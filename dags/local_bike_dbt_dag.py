from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode
from pendulum import datetime

DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/local_bike")
DBT_PROFILES_PATH = Path("/usr/local/airflow/.dbt_profiles")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_PATH / "profiles.yml",
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    install_dbt_deps=True,
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/bin/dbt",
)

render_config = RenderConfig(
    load_method=LoadMode.DBT_LS,
)


@dag(
    dag_id="local_bike_dbt",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["dbt", "bigquery"],
)
def local_bike_pipeline():
    start = EmptyOperator(task_id="start")

    dbt_group = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
    )

    end = EmptyOperator(task_id="end")

    start >> dbt_group >> end


local_bike_pipeline()
