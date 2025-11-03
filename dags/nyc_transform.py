from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROJECT_PATH = "/Users/dheaanggita/nyc_taxi_project/dbt_project"

default_args = {
    'owner': 'dhea',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG("nyc_taxi_transform",
         default_args=default_args,
         start_date=datetime(2025, 11, 1),
         schedule_interval='0 6 * * *',  
         catchup=False) as dag:
    
    t1_dbt_seed = BashOperator(
        task_id='dbt_seed_zones',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt seed --profile nyc_taxi --target dev'
    )

    t2_dbt_run = BashOperator(
        task_id='dbt_run_transformation',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt run --profile nyc_taxi --target dev'
    )

    t3_dbt_test = BashOperator(
        task_id='dbt_run_tests',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt test --profile nyc_taxi --target dev'
    )

    t4_dbt_docs = BashOperator(
        task_id='dbt_generate_docs',
        bash_command=f'cd {DBT_PROJECT_PATH} && dbt docs generate --profile nyc_taxi --target dev'
    )

    t1_dbt_seed >> t2_dbt_run >> t3_dbt_test >> t4_dbt_docs