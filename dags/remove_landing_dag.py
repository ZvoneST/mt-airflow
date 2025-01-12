import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime


remove_soil_data = 'DELETE FROM landing.soil_data;'
remove_meteo_data = 'DELETE FROM landing.meteo_data CASCADE;'
remove_agro_meteo_data = 'DELETE FROM landing.agro_meteo_data CASCADE;'

default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='remove_landing',
    default_args=default_args,
    schedule='0 20 * * *',
    catchup=True
) as dag:

    remove_soil_data = SQLExecuteQueryOperator(
        task_id='remove_soil_data',
        conn_id='raw_zone_conn',
        sql=remove_soil_data
    )
    
    remove_meteo_data = SQLExecuteQueryOperator(
        task_id='remove_meteo_data',
        conn_id='raw_zone_conn',
        sql=remove_meteo_data
    )
    
    remove_agro_meteo_data = SQLExecuteQueryOperator(
        task_id='remove_agro_meteo_data',
        conn_id='raw_zone_conn',
        sql=remove_agro_meteo_data
    )
    
    remove_soil_data >> remove_meteo_data >> remove_agro_meteo_data
    

    
    