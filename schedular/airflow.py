from __future__ import annotations

import subprocess
from typing import List
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from logs.alert import broadcast_alert


def task_failure_callback(context):
    """
    Airflow callback to send Telegram and Discord notifications on task failure.
    """
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "Unknown Task"
    exception = context.get("exception")
    error_message = str(exception) if exception else "Task failed without exception details."
    
    broadcast_alert(
        step_name=f"Airflow Task: {task_id}", 
        message=error_message,
        is_success=False
    )


def task_success_callback(context):
    """
    Airflow callback to send Telegram and Discord notifications on task success.
    """
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "Unknown Task"
    
    broadcast_alert(
        step_name=f"Airflow Task: {task_id}", 
        message="Task completed successfully without errors.",
        is_success=True
    )


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
    "on_success_callback": task_success_callback,
}


@dag(
    dag_id="Earnest_ETL_Workflow",
    default_args=default_args,
    description="DAG for ETL workflow starting from running batch PySpark processing, followed by DBT modeling.",
    schedule_interval="@weekly",        
    start_date=datetime(2026, 1, 1),  
    catchup=False,                   
    tags=["Earnest ETL", "batch", "streaming", "pyspark", "dbt", "ETL", "workflow", "sql", "ali", "abdelhalim"],
)
def etl_workflow():
    """
    ### Earnest ETL Airflow DAG
    This DAG orchestrates the data engineering pipeline for the Earnest project.
    
    **Workflow Steps:**
    1. **Extract**: Executes the data extraction/ingestion modes by running `main.py --mode batch`.
    2. **Transform & Load (T&L)**: Executes the PySpark transformations and loads the data.
    3. **Modeling**: Executes `dbt run` to build analytical models in the data warehouse.
    """

    @task()
    def etl(**context) -> bool:
        """
        Executes the data extraction process.
        
        This task dynamically chooses the mode based on how the DAG was triggered:
        - Scheduled (Weekly): Runs the "streaming" pipeline.
        - Manual (User Triggered): Runs the "batch" pipeline by default (or reads custom config).
            
        Returns:
            bool: Status message upon successful completion.
        """
        dag_run = context.get("dag_run")
        
        if dag_run and dag_run.run_type == "manual":
            jobs = dag_run.conf.get("jobs", ["batch"]) if dag_run.conf else ["batch"]
            print(f"Manual trigger detected. Running jobs: {jobs}")
        else:
            jobs = ["streaming"]
            print(f"Scheduled run detected. Running jobs: {jobs}")

        for job in jobs:
            print(f"Starting extraction for mode: {job}")
            if job in ["streaming", "batch"]:
                result = subprocess.run(
                    ["python3", "main.py", "--mode", job],
                    capture_output=True,
                    text=True
                )
                
                if result.returncode != 0:
                    raise Exception(f"Extraction failed for {job} mode.\nError output:\n{result.stderr}")
                
                print(f"Successfully completed {job} mode.\nOutput:\n{result.stdout}")
            else:
                raise ValueError(f"Invalid job name: {job}. Must be 'streaming' or 'batch'.")
                
        return True

    @task()
    def modeling(input_status: bool):
        """
        Executes DBT models to transform the data in the warehouse (Snowflake).
        
        Args:
            input_status (bool): The status from the upstream T&L task.
        """
        print(f"Upstream status: {input_status}")
        print("Running DBT models...")
        
        result = subprocess.run(
            ["dbt", "build"],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            raise Exception(f"DBT build failed.\nError output:\n{result.stderr}")
            
        print(f"DBT completed successfully.\nOutput:\n{result.stdout}")
 
    data = etl()
    modeling(data)


etl_workflow_dag = etl_workflow()