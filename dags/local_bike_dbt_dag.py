from functools import partial
from pathlib import Path

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.providers.smtp.notifications.smtp import SmtpNotifier
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode
from cosmos.operators.local import DbtDocsGCSLocalOperator
from pendulum import datetime

from include.dbt_monitor import capture_run_results, send_pipeline_report

DBT_PROJECT_PATH = Path("/usr/local/airflow/dbt/local_bike")
DBT_PROFILES_PATH = Path("/usr/local/airflow/.dbt_profiles")

# Bucket GCS où les fichiers de doc seront uploadés.
# Activer le website hosting sur ce bucket : gsutil web set -m index.html gs://BUCKET_NAME
GCS_BUCKET = "local-bike-dbt-docs"
GCS_FOLDER = "docs"

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


def notify_slack(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url
    SlackWebhookHook(slack_webhook_conn_id="slack_webhook").send_text(
        f":red_circle: *DAG échoué*\n"
        f"*DAG*: {dag_id}\n"
        f"*Task*: {task_id}\n"
        f"*Logs*: {log_url}"
    )


notify_email = SmtpNotifier(
    to="jerem9911@hotmail.com",
    subject="[Airflow] Échec du DAG {{ dag.dag_id }}",
    html_content=(
        "<h3>Le DAG <b>{{ dag.dag_id }}</b> a échoué.</h3>"
        "<p><b>Task :</b> {{ task_instance.task_id }}</p>"
        "<p><a href='{{ task_instance.log_url }}'>Voir les logs</a></p>"
    ),
)


@dag(
    dag_id="local_bike_dbt",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={
        "retries": 2,
        "on_failure_callback": [notify_slack, notify_email],
    },
    tags=["dbt", "bigquery"],
)
def local_bike_pipeline():
    start = EmptyOperator(task_id="start")

    staging = DbtTaskGroup(
        group_id="staging",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:models/staging"],
        ),
    )

    intermediate = DbtTaskGroup(
        group_id="intermediate",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:models/intermediate"],
        ),
    )

    mart = DbtTaskGroup(
        group_id="mart",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=["path:models/mart"],
        ),
    )

    monitor_staging = PythonOperator(
        task_id="monitor_staging",
        python_callable=partial(capture_run_results, "staging"),
    )

    monitor_intermediate = PythonOperator(
        task_id="monitor_intermediate",
        python_callable=partial(capture_run_results, "intermediate"),
    )

    monitor_mart = PythonOperator(
        task_id="monitor_mart",
        python_callable=partial(capture_run_results, "mart"),
    )

    generate_docs = DbtDocsGCSLocalOperator(
        task_id="generate_docs",
        project_dir=DBT_PROJECT_PATH,
        profile_config=profile_config,
        execution_config=execution_config,
        # Connexion Airflow de type "Google Cloud" avec les droits Storage Object Admin
        connection_id="google_cloud_default",
        bucket_name=GCS_BUCKET,
        folder_dir=GCS_FOLDER,
        # Overwrite les fichiers existants à chaque run pour garder la doc à jour
        overwrite_existing_files=True,
    )

    report_pipeline = PythonOperator(
        task_id="report_pipeline",
        python_callable=send_pipeline_report,
        trigger_rule="all_done",
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> staging >> monitor_staging
        >> intermediate >> monitor_intermediate
        >> mart >> monitor_mart
        >> [generate_docs, report_pipeline]
        >> end
    )


local_bike_pipeline()
