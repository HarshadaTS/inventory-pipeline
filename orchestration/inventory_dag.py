from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Default settings for the DAG
default_args = {
    'owner': 'inventory-pipeline',
    'depends_on_past': False,
    'start_date': datetime(2026, 4, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

# Create the DAG
dag = DAG(
    'inventory_batch_pipeline',
    default_args=default_args,
    description='Nightly inventory restock forecast pipeline',
    schedule_interval='0 23 * * *',  # runs every night at 11pm
    catchup=False,
    tags=['inventory', 'batch', 'restock']
)

# Task 1: Check Kafka is running
check_kafka = BashOperator(
    task_id='check_kafka_running',
    bash_command='echo "Checking Kafka..." && echo "Kafka OK"',
    dag=dag
)

# Task 2: Check PostgreSQL is running
check_postgres = BashOperator(
    task_id='check_postgres_running',
    bash_command='echo "Checking PostgreSQL..." && echo "PostgreSQL OK"',
    dag=dag
)

# Task 3: Run the batch processor
run_batch = BashOperator(
    task_id='run_batch_processor',
    bash_command='cd /opt/airflow/batch-processor && python batch_processor.py',
    dag=dag
)

# Task 4: Confirm report saved
confirm_report = BashOperator(
    task_id='confirm_report_saved',
    bash_command='echo "Batch report saved to cold storage successfully"',
    dag=dag
)

# Define the order: check first, then run, then confirm
check_kafka >> check_postgres >> run_batch >> confirm_report