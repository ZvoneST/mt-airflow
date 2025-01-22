import sys
import os
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from include.api_processor import APIProcessor
from include.request_methods import meteo_data_request

from datetime import datetime
from airflow.models import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator

source_conn = BaseHook.get_connection('farm_management_conn')
target_conn = BaseHook.get_connection('raw_zone_conn')

api_conn = BaseHook.get_connection('meteo_data_conn')

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

meteo_query = '''
    SELECT meteo_location_id, meteo_coordinates_wkt 
    FROM management.meteo_locations;
'''
insert_query = '''
    INSERT INTO landing.meteo_data (meteo_location_id, meteo_data)
    VALUES (%s, %s)
'''

api_processor = APIProcessor(source_conn=conn_fm, target_conn=conn_landing,
                             fetch_query=meteo_query, insert_query=insert_query,
                             request_method=meteo_data_request, api_key=api_conn.login)

default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='meteo_data_raw',
    default_args=default_args,
    schedule='30 1 * * *',
    catchup=False
) as dag:
    
    meteo_api_active = HttpSensor(
        task_id='meteo_api_active',
        http_conn_id='meteo_data_conn',
        endpoint='api/',
    )
    
    get_store_meteo_data = PythonOperator(
        task_id='get_store_meteo_data',
        python_callable=api_processor.process_data
    )
    
    meteo_api_active >> get_store_meteo_data