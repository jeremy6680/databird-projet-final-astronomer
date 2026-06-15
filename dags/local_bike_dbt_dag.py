from pathlib import Path

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode
from pendulum import datetime

DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/local_bike")
DBT_PROFILES_PATH = Path("/usr/local/airflow/.dbt_profiles")

profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_PATH / "profiles.yml",
)

dbt_dag = DbtDag(
    dag_id="local_bike_dbt",
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
        install_dbt_deps=True,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="/usr/local/bin/dbt",
        dbt_cmd_global_flags=["--full-refresh"],
    ),
    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
    ),
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 2},
    tags=["dbt", "bigquery"],
)
