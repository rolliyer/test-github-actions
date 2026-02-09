from __future__ import annotations

import os
import yaml
from datetime import datetime, timezone
import logging
import json

from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.trigger_rule import TriggerRule

from operators.roller_cloud_run_operator import RollerCloudRunExecuteJobOperator
from operators.workato_job_poll_sensor import WorkatoJobPollSensor

from utils.dag_config_loader import load_config
from utils.dag_logger import get_logger

logger = get_logger(__name__)


DAG_ID = "sfdc_bronze_ingestion"
CONFIG_FILE_NAME = "sfdc_bronze_config.yaml"

config = load_config(__file__, CONFIG_FILE_NAME)

SFDC_EXTRACT_POOL = "sfdc_extract_pool"
GCP_CONN_ID = "google_cloud_default"

API_TOKEN_CONN_ID = Variable.get("WORKATO_API_TOKEN")
API_ENDPOINT = Variable.get("WORKATO_API_ENDPOINT")

POLLING_API_ENDPOINT = Variable.get("WORKATO_POLLING_ENDPOINT")
ENVIRONMENT = Variable.get("ENVIRONMENT", "dev")

# --- For start time ---
now = datetime.now(timezone.utc)
year, month, day, hour = now.year, now.month, now.day, now.hour
end_time = now.strftime("%Y-%m-%dT%H:%M:%S+0000")

PROJECT_ID = f"{config.get('project_id')}-{ENVIRONMENT}"
JOB_NAME = f"{config.get('job_name')}-{ENVIRONMENT}"
BUCKET_NAME = f"{config.get('bucket_name')}-{ENVIRONMENT}"
REGION = f"{config.get('region')}"
SCHEDULE = None if ENVIRONMENT == 'dev' else config.get('schedule')


def decide_to_poll_or_ingest(task_instance, **kwargs):
    upstream_task_id = list(task_instance.task.upstream_task_ids)[0]
    sfdc_response_str = task_instance.xcom_pull(task_ids=upstream_task_id)

    try:
        sfdc_response = json.loads(sfdc_response_str)

        if 'record_id' in sfdc_response and 'status' in sfdc_response:
            task_instance.xcom_push(key='record_id', value=sfdc_response['record_id'])
            return f"poll_{sfdc_response['object_name']}"
        
        return f"no_op_{sfdc_response['object_name']}"

    except (json.JSONDecodeError, TypeError, KeyError) as e:
        logger.warning(f"Could not parse response or find keys: {e}. Assuming no polling is needed.")
        obj_name = upstream_task_id.split('extract_')[-1]
        return f"no_op_{obj_name}"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 11, 28),
    schedule=SCHEDULE,
    catchup=False,
    default_args={'retries': 0},
    is_paused_upon_creation=True,
    tags=["salesforce", "bronze", "cloudrun", "polling"],
    render_template_as_native_obj=True,
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED)
    failure = BashOperator(
        task_id="failure",
        bash_command=f'echo "A task failed in the {DAG_ID} DAG. Check logs for details."',
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    sfdc_objects = config.get("sfdc_objects", [])
    
    if not sfdc_objects:
        start >> end
    else:
        all_tasks = []
        for obj_config in sfdc_objects:
            obj_name = obj_config["name"]
            
            record_timestamp_column = obj_config.get("record_timestamp_column") or config.get("record_timestamp_column", "SystemModstamp")

            extract_task = HttpOperator(
                task_id=f"extract_{obj_name}",
                http_conn_id="workato_http_base",
                endpoint=API_ENDPOINT,
                method="GET",
                trigger_rule='all_done',
                headers={
                    "Content-Type": "application/json",
                    "api-token": API_TOKEN_CONN_ID,
                },
                data={
                    "object_name": obj_config["object_name"],
                    "record_timestamp_column": record_timestamp_column,
                    "next_watermark_time": end_time,
                    "bucket_name": BUCKET_NAME
                },
                log_response=True,
                response_check=lambda response: response.status_code in [200, 202],
                # Assign to the pool to limit concurrency
                pool=SFDC_EXTRACT_POOL,
            )

            branch_task = BranchPythonOperator(
                task_id=f'decide_poll_or_ingest_{obj_name}',
                python_callable=decide_to_poll_or_ingest,
            )

            record_id_for_polling = f"{{{{ task_instance.xcom_pull(task_ids='decide_poll_or_ingest_{obj_name}', key='record_id') }}}}"

            polling_task = WorkatoJobPollSensor(
                task_id=f'poll_{obj_name}',
                http_conn_id='workato_http_base',
                record_id=record_id_for_polling,
                api_token=API_TOKEN_CONN_ID,
                polling_endpoint=POLLING_API_ENDPOINT,
                poke_interval=30,
                timeout=3600,
                mode='poke'
            )
            
            no_op_task = EmptyOperator(task_id=f'no_op_{obj_name}')

            join_task = EmptyOperator(task_id=f'join_{obj_name}', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

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

            start >> extract_task >> branch_task
            branch_task >> [polling_task, no_op_task]
            polling_task >> join_task
            no_op_task >> join_task
            join_task >> ingest_task
            ingest_task >> end
            
            all_tasks.extend([extract_task, branch_task, polling_task, no_op_task, join_task, ingest_task])

        if all_tasks:
            failure.set_upstream(all_tasks)

