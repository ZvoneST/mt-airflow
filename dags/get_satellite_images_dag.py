from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 18),
    'retries': 1,
}


with DAG(
    dag_id='get_satellite_images',
    default_args=default_args,
    schedule_interval=timedelta(days=6),
    catchup=True
) as dag:
    
    get_images = BashOperator(
        task_id='get_images',
        bash_command=(
            "docker run --rm "
            "--env-file /home/zvone/env_vars/.mt_sat_env "
            "-v /home/zvone/mtsi/sentinel_images/landing/:/home/mtsi/sentinel_images/landing/ "
            "mt-satellite"
        ),
    )
    
    get_images