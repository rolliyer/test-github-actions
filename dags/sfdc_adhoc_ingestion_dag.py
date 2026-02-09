from __future__ import annotations

import os
import yaml
from datetime import datetime, timezone
import logging

from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.utils.trigger_rule import TriggerRule

from operators.roller_cloud_run_operator import RollerCloudRunExecuteJobOperator

from utils.dag_config_loader import load_config
from utils.dag_logger import get_logger

logger = get_logger(__name__)


DAG_ID = "sfdc_adhoc_ingestion"
CONFIG_FILE_NAME = "sfdc_bronze_config.yaml"

config = load_config(__file__, CONFIG_FILE_NAME)

SFDC_EXTRACT_POOL = "sfdc_extract_pool"

GCP_CONN_ID = "google_cloud_default"

API_TOKEN_CONN_ID = Variable.get("WORKATO_API_TOKEN")
ENVIRONMENT = Variable.get("ENVIRONMENT", "dev")

API_ENDPOINT = Variable.get("WORKATO_ADHOC_API_ENPOINT")

PROJECT_ID = f"{config.get('project_id')}-{ENVIRONMENT}"
JOB_NAME = f"{config.get('job_name')}-{ENVIRONMENT}"
BUCKET_NAME = f"{config.get('bucket_name')}-{ENVIRONMENT}"
REGION = f"{config.get('region')}"
sfdc_objects = config.get("sfdc_objects", [])

# --- For start & end time ---
start_time = Variable.get("start_time")
end_time = Variable.get("end_time")
sfdc_object_list = Variable.get("sfdc_object_list", deserialize_json=True)

# for load type
load_type = Variable.get("sfdc_load_type", default_var="full_load").lower()

def parse_end_time(end_time_str):
    if "T" in end_time_str:
        # Full datetime format
        return datetime.strptime(end_time_str, "%Y-%m-%dT%H:%M:%S%z")
    else:
        # Date only format - set to 23:59:59 UTC
        dt = datetime.strptime(end_time_str, "%Y-%m-%d")
        return dt.replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)

end_datetime = parse_end_time(end_time)
# Extract year, month, day, and hour
year, month, day, hour = end_datetime.year, end_datetime.month, end_datetime.day, end_datetime.hour

# branch method
def branch_sfdc_objects():
    """
    Determines which Salesforce object tasks should run
    based on Airflow Variable vs YAML configuration.
    """
    user_sfdc_object_list = {sfdc_object.strip().lower() for sfdc_object in sfdc_object_list}
    # Build task_ids to run
    tasks_to_run = [
        f"extract_{obj['name']}"
        for obj in sfdc_objects
        if obj["name"].strip().lower() in user_sfdc_object_list
    ]

    # Safety fallback
    if not tasks_to_run:
        return "no_objects_selected"

    return tasks_to_run

# DAG definition
with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 11, 28),
    schedule=None,
    catchup=False,
    default_args={'retries': 0},
    is_paused_upon_creation=True,
    tags=["salesforce", "bronze", "cloudrun", "adhocLoad"],
    render_template_as_native_obj=True,
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)
    failure = BashOperator(
        task_id="failure",
        bash_command=f'echo "A task failed in the {DAG_ID} DAG. Check logs for details."',
        trigger_rule=TriggerRule.ONE_FAILED,
    )
    branch_task = BranchPythonOperator(
        task_id="select_sfdc_objects",
        python_callable=branch_sfdc_objects,
    )
    
    no_objects_selected = EmptyOperator(task_id="no_objects_selected")
    
    extract_tasks = []
    ingest_tasks = []
    for obj_config in sfdc_objects:
        record_timestamp_column = obj_config.get("record_timestamp_column") or config.get("record_timestamp_column", "SystemModstamp")
        obj_name = obj_config["name"]

        extract_task = HttpOperator(
            task_id=f"extract_{obj_name}",
            http_conn_id="workato_http_base",
            endpoint=API_ENDPOINT,
            method="GET",
            trigger_rule="all_done",
            headers={
                "Content-Type": "application/json",
                "api-token": API_TOKEN_CONN_ID,
            },
            data={
                "object_name": obj_config["object_name"],
                "record_timestamp_column": record_timestamp_column,
                "start_time": start_time,
                "end_time": end_time,
                "bucket_name": BUCKET_NAME,
                "load_type": load_type,
            },
            log_response=True,
            response_check=lambda response: response.status_code in [200, 202],
            pool=SFDC_EXTRACT_POOL,
        )

        ingest_task = RollerCloudRunExecuteJobOperator(
            task_id=f"ingest_{obj_name}",
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            gcp_conn_id=GCP_CONN_ID,
            polling_period_seconds=30,
            overrides={
                "container_overrides": [{
                    "args": [
                        "--source=salesforce",
                        f"--object_name={obj_config['cloudrun_object_type']}",
                        f"--year={year}",
                        f"--month={month}",
                        f"--day={day}",
                        f"--hour={hour}",
                    ],
                    "env": [
                        {"name": "RUN_ID", "value": "{{ run_id }}"},
                        {"name": "LOGICAL_DATE", "value": "{{ ds }}"},
                        {"name": "DAG_ID", "value": "{{ dag.dag_id }}"},
                        {"name": "TASK_ID", "value": "{{ task.task_id }}"},
                    ],
                }]
            },
        )
        extract_tasks.append(extract_task)
        ingest_tasks.append(ingest_task)

        extract_task >> ingest_task
        
    start >> branch_task

    branch_task >> extract_tasks
    branch_task >> no_objects_selected

    ingest_tasks >> end
    no_objects_selected >> end
    
    # Collect all tasks that can fail to set the global failure trigger
    tasks_to_monitor = [branch_task, no_objects_selected] + extract_tasks + ingest_tasks
    failure.set_upstream(tasks_to_monitor)