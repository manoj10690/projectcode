"""
Cloud Composer DAG -- scheduled orchestration for the metadata-driven platform.

Complements the event-driven Cloud Workflows path (orchestration/workflows.yaml):
use Workflows for "run as soon as a file lands", use this DAG for "run every
pipeline nightly with retries/backfill/SLA monitoring", per Ch.5.1.1.

Deploy: upload to the Composer environment's DAGs GCS bucket, or
  gcloud composer environments storage dags import \
    --environment <ENV_NAME> --location us-central1 --source orchestration/airflow_dag.py
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

PROJECT_ID = "{{ var.value.project_id }}"
REGION = "us-central1"
DLQ_BUCKET = f"gs://{PROJECT_ID}-dev-quarantine-dlq"
TEMPLATE_PATH = f"gs://{PROJECT_ID}-dev-df-staging/templates/ingest-engine.json"

default_args = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def list_active_pipelines(**context):
    """Reads active pipelines from the metadata store so the DAG can fan out
    a Dataflow task per pipeline without hard-coding them."""
    hook = PostgresHook(postgres_conn_id="ingest_metadata_db")
    rows = hook.get_records(
        "SELECT pipeline_id, source_config FROM pipelines WHERE is_active = TRUE"
    )
    pipeline_ids = [str(r[0]) for r in rows]
    context["ti"].xcom_push(key="pipeline_ids", value=pipeline_ids)
    return pipeline_ids


with DAG(
    dag_id="metadata_driven_ingestion_nightly",
    description="Nightly fan-out: launch the generic Beam engine once per active pipeline",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # 02:00 daily
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "metadata-driven", "gcp"],
) as dag:

    discover_pipelines = PythonOperator(
        task_id="discover_active_pipelines",
        python_callable=list_active_pipelines,
    )

    # NOTE: in Airflow 2.x this would typically be expanded dynamically with
    # .expand() over the XCom list; shown here as a single representative task
    # for the sample "customer_orders_feed" pipeline registered in
    # sql/02_sample_metadata.sql -- clone this task per production pipeline_id
    # or switch to dynamic task mapping.
    run_customer_orders_ingest = DataflowStartFlexTemplateOperator(
        task_id="run_customer_orders_ingest",
        project_id=PROJECT_ID,
        location=REGION,
        body={
            "launchParameter": {
                "jobName": "ingest-customer-orders-{{ ds_nodash }}",
                "containerSpecGcsPath": TEMPLATE_PATH,
                "parameters": {
                    "pipeline_id": "11111111-1111-1111-1111-111111111111",
                    "input": f"gs://{PROJECT_ID}-dev-bronze/incoming/customer_orders/*.json",
                    "dlq_path": DLQ_BUCKET,
                    "output_table": f"{PROJECT_ID}:silver.customer_orders",
                    "db_host": "{{ var.value.metadata_db_host }}",
                    "db_name": "ingest_metadata",
                    "db_user": "ingest_app",
                    "db_password": "{{ var.value.metadata_db_password }}",
                },
            }
        },
    )

    materialize_gold_layer = BigQueryInsertJobOperator(
        task_id="materialize_gold_daily_summary",
        configuration={
            "query": {
                "query": "{% include 'sql_templates/merge_daily_order_summary.sql' %}",
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    discover_pipelines >> run_customer_orders_ingest >> materialize_gold_layer
