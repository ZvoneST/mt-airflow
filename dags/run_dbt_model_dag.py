from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

with DAG(
    dag_id='run_dbt_model_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="""
        source ~/.virtualenv/mt-dwh/bin/activate && \
        cd ~/repos/dwh/agro_dwh && \
        dbt run --profiles-dir ~/repos/dwh/profiles.yml && \
        dbt test --profiles-dir ~/repos/dwh/profiles.yml 
        """
    )
        
    dbt_run