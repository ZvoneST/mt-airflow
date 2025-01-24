import sys
import os
import psycopg2
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from include.vegetation_index_processor import VegetationIndexProcessor

source_conn = BaseHook.get_connection('raw_zone_conn')
dest_conn = BaseHook.get_connection('exploration_zone_conn')
user = source_conn.login
uspass = source_conn.password

exploration_conn = psycopg2.connect(
        dbname=dest_conn.schema,
        user=dest_conn.login,
        password=dest_conn.password,
        host=dest_conn.host,
        port=dest_conn.port
)

processor = VegetationIndexProcessor(db_connection=exploration_conn)


default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 18),
    'retries': 1,
}
        

landing_to_raw = '''
    INSERT INTO raw.satellite_metadata (field_id, image_path, response_meta_path)
    SELECT
        field_id,
        REPLACE(image_path, '/landing/', '/raw_zone/') AS image_path,
        REPLACE(response_meta_path, '/landing/', '/raw_zone/') AS response_meta_path
    FROM landing.satellite_metadata;
'''

raw_to_exploration = f'''
    INSERT INTO external_data.satellite_metadata (sat_metadata_id, field_id, image_path, response_meta_path, created_on)
    SELECT
        source_data.sat_metadata_id,
        source_data.field_id,
        source_data.image_path,
        source_data.response_meta_path,
        source_data.created_on
    FROM dblink(
        'dbname=raw_zone user={user} password={uspass} host=localhost',
        'SELECT sat_metadata_id, field_id, image_path, response_meta_path, created_on FROM raw.satellite_metadata'
    ) AS source_data(sat_metadata_id INTEGER, field_id SMALLINT, image_path TEXT, response_meta_path TEXT, created_on TIMESTAMP)
    WHERE NOT EXISTS (
        SELECT 1
        FROM external_data.satellite_metadata dest
        WHERE dest.sat_metadata_id = source_data.sat_metadata_id
    );
'''


with DAG(
    dag_id='get_satellite_images',
    default_args=default_args,
    schedule_interval=timedelta(days=6, minutes=30),
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
) as dag:
    
    get_images = BashOperator(
        task_id='get_images',
        bash_command=(
            "docker run --rm \
            --env-file ~/env_vars/.mt_sat_env \
            -v ~/mtsi/sentinel_images/landing/:/home/mtsi/sentinel_images/landing/ \
            mt-satellite:ms"
        )
    )
    
    transfer_satellite_meta_data_raw = SQLExecuteQueryOperator(
        task_id='transfer_satellite_meta_data_raw',
        conn_id='raw_zone_conn',
        sql=landing_to_raw
    )
    
    transfer_landing_images = BashOperator(
        task_id='transfer_landing_images',
        bash_command=("cp -R ~/mtsi/sentinel_images/landing/* ~/mtsi/sentinel_images/raw_zone/")
    )
    
    transfer_satellite_meta_exploration = SQLExecuteQueryOperator(
        task_id='transfer_satellite_meta_exploration',
        conn_id='exploration_zone_conn',
        sql=raw_to_exploration
    )
    
    remove_landing_images = BashOperator(
        task_id='remove_landing_images',
        bash_command=("rm -rf ~/mtsi/sentinel_images/landing/*")
    )
        
    remove_landing_metadata = SQLExecuteQueryOperator(
        task_id='remove_landing_metadata',
        conn_id='raw_zone_conn',
        sql='TRUNCATE TABLE landing.satellite_metadata;'
    )
    
    extract_vegetation_indices = PythonOperator(
        task_id='extract_vegetation_indices',
        python_callable=processor.process
    )
    
    get_images >> transfer_satellite_meta_data_raw >> transfer_landing_images \
    >> transfer_satellite_meta_exploration >> remove_landing_images >> \
    remove_landing_metadata >> extract_vegetation_indices