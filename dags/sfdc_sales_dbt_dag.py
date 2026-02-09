from __future__ import annotations

import os
import yaml
from datetime import datetime

from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.utils.trigger_rule import TriggerRule

from utils.dag_config_loader import load_config
from utils.dag_logger import get_logger

logger = get_logger(__name__)

DAG_ID = "sfdc_sales_dbt_dag"
CONFIG_FILE_NAME = "sfdc_sales_dbt_dag_config.yaml"

config = load_config(__file__, CONFIG_FILE_NAME)

# --- dbt Cloud Configuration ---
DBT_CLOUD_CONN_ID = config.get("dbt_cloud_conn_id", "dbt_cloud_default")
DBT_ACCOUNT_ID = config.get("dbt_account_id")

ENVIRONMENT = Variable.get("ENVIRONMENT", "dev")
env_config = config.get("environments", {}).get(ENVIRONMENT, [])

JOB_POLLING_INTERVAL = int(Variable.get("DBT_JOB_POLLING_INTERVAL", 20))
SCHEDULE = None if ENVIRONMENT == 'dev' else config.get('schedule')


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 12, 31),
    schedule=SCHEDULE,
    catchup=False,
    default_args={
        "dbt_cloud_conn_id": DBT_CLOUD_CONN_ID,
        "account_id": DBT_ACCOUNT_ID,
        "retries": 0,
    },
    is_paused_upon_creation=True,
    tags=["dbt", "dbt-cloud", "sales"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)
    failure = EmptyOperator(task_id="failure", trigger_rule=TriggerRule.ONE_FAILED)

    dbt_tasks = []
    for job in env_config:
        task = DbtCloudRunJobOperator(
            task_id=job["task_name"],
            job_id=job["job_id"],
            check_interval=JOB_POLLING_INTERVAL,
            timeout=600,
            wait_for_termination=True,
        )
        dbt_tasks.append(task)

    if dbt_tasks:
        start >> dbt_tasks[0]
        for i in range(len(dbt_tasks) - 1):
            dbt_tasks[i] >> dbt_tasks[i+1]
        dbt_tasks[-1] >> end
        failure.set_upstream(dbt_tasks)