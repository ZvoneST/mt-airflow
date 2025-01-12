import sys
import os
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from include.api_processor import APIProcessor
from include.request_methods import soilgrids_request
from include.structured_raw_soil_grid_query import structured_raw_soil_grid_query

from datetime import datetime
from airflow.models import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

source_conn = BaseHook.get_connection('farm_management_conn')
target_conn = BaseHook.get_connection('raw_zone_conn')

conn_fm = psycopg2.connect(
    dbname=source_conn.schema,
    user=source_conn.login,
    password=source_conn.password,
    host=source_conn.host,
    port=source_conn.port,
)

conn_landing = psycopg2.connect(
    dbname=target_conn.schema,
    user=target_conn.login,
    password=target_conn.password,
    host=target_conn.host,
    port=target_conn.port,
)

field_query = '''
    SELECT field_id, centroid_coordinates_wkt 
    FROM management.fields
    WHERE soil_grid_imported = false;
'''
insert_query = '''
    INSERT INTO landing.soil_data (field_id, soil_grid_data)
    VALUES (%s, %s)
    ON CONFLICT (field_id) DO UPDATE
    SET soil_grid_data = EXCLUDED.soil_grid_data;
'''
update_query = '''
    UPDATE management.fields
    SET soil_grid_imported = true
    WHERE soil_grid_imported = false
'''

transfer_query = '''
    INSERT INTO raw.soil_data (field_id, soil_grid_data)
    SELECT field_id, soil_grid_data
    FROM landing.soil_data AS source_data
    WHERE NOT EXISTS (
        SELECT 1
        FROM raw.soil_data dest
        WHERE dest.field_id = source_data.field_id
    );
'''

api_processor = APIProcessor(source_conn=conn_fm, target_conn=conn_landing,
                             fetch_query=field_query, insert_query=insert_query,
                             request_method=soilgrids_request)

default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='soil_grid_raw',
    default_args=default_args,
    schedule='15 1 * * *',
    catchup=False
) as dag:
    
    soilgrid_api_active = HttpSensor(
        task_id='soilgrid_api_active',
        http_conn_id='soil_grid_api_conn',
        endpoint='/v2.0/docs'
    )
    
    get_store_soil_grids = PythonOperator(
        task_id='get_store_soil_grids',
        python_callable=api_processor.process_data
    )
    
    update_source_data = SQLExecuteQueryOperator(
        task_id='update_source_data',
        conn_id='farm_management_conn',
        sql=update_query
    )
    
    structured_raw_soil_data = SQLExecuteQueryOperator(
        task_id='structured_raw_soil_data',
        conn_id='raw_zone_conn',
        sql=structured_raw_soil_grid_query
    )
    
    soilgrid_api_active >> get_store_soil_grids >> update_source_data >> \
        structured_raw_soil_data