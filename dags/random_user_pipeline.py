from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests
import json
import os
import sys
import pandas as pd
from airflow.operators.bash import BashOperator

# For local VS Code IntelliSense
sys.path.append(os.path.join(os.path.dirname(__file__), '../include'))

# For Docker execution
sys.path.append('/opt/airflow/include')

from postgres_engine import insert_idempotent_bronze

default_args = {
    'owner': 'data_engineer',
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,         
    'email_on_retry': False,          
    'email': ['levinne521@gmail.com']
}

@dag(
    schedule='0 */2 * * *',
    start_date=datetime(2026, 10, 3),
    catchup=False,
    default_args=default_args,
    tags=['bronze-layer', 'api-ingestion', 'idempotent', 'dbt']
)
def random_user_bronze_ingestion():

    @task()
    def extract_from_api(ds=None):
        """Pulls nested JSON from the API and stages it."""
        response = requests.get('https://randomuser.me/api/?results=50')
        response.raise_for_status()
        
        file_path = f'/tmp/api_batch_{ds}.json'
        
        with open(file_path, 'w') as f:
            json.dump(response.json()['results'], f)
            
        return file_path

    @task()
    def process_and_load_bronze(file_path: str, ds=None):
        """Flattens the JSON, adds metadata, and triggers DB logic."""
        with open(file_path, 'r') as f:
            raw_json = json.load(f)
            
        # 1. Flatten the API data
        df = pd.json_normalize(raw_json)
        df.columns = df.columns.str.replace('.', '_')
        
        # 2. Add Immutable Pipeline Metadata
        df['_extracted_date'] = ds
        
        # 3. Generate Deterministic Surrogate Hash (Idempotency Key)
        # Using login_uuid as the source's natural key
        df['_pipeline_record_hash'] = pd.util.hash_pandas_object(
            df[['login_uuid', '_extracted_date']], index=False
        ).astype(str)
        
        # 4. Push to Database
        insert_idempotent_bronze(table_name='raw_users_bronze', df=df)
        
        return True

    @task()
    def cleanup_staging(file_path: str, load_success: bool):
        """Removes the temp file to protect worker disk space."""
        if load_success and os.path.exists(file_path):
            os.remove(file_path)

    # dbt Tasks
    # 1. Silver Transformation (ONLY runs silver_users.sql)
    dbt_transform_silver = BashOperator(
        task_id='dbt_transform_silver',
        bash_command='dbt run --select silver_users --project-dir /opt/airflow/include/dbt_project --profiles-dir /opt/airflow/include/dbt_project',
    )

    # 2. Silver Testing (ONLY tests silver_users.sql)
    dbt_test_silver = BashOperator(
        task_id='dbt_test_silver',
        bash_command='dbt test --select silver_users --project-dir /opt/airflow/include/dbt_project --profiles-dir /opt/airflow/include/dbt_project',
    )

    # 3. Gold Transformation (ONLY runs gold_user_demographics.sql)
    dbt_transform_gold = BashOperator(
        task_id='dbt_transform_gold',
        bash_command='dbt run --select gold_user_demographics --project-dir /opt/airflow/include/dbt_project --profiles-dir /opt/airflow/include/dbt_project',
    )

    
    # Task Graph Definition
    staged_file = extract_from_api()
    load_status = process_and_load_bronze(staged_file)
    cleanup_task = cleanup_staging(staged_file, load_status)

    # Set the execution pipeline order
    load_status >> dbt_transform_silver >> dbt_test_silver >> dbt_transform_gold >> cleanup_task

# Instantiate DAG
dag_instance = random_user_bronze_ingestion()