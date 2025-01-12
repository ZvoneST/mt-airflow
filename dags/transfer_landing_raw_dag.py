import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from include.landing_raw_transfer_queries import transfer_landing_raw_queries

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime


default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

queries = transfer_landing_raw_queries()

with DAG(
    dag_id='landing_raw_transfer',
    default_args=default_args,
    schedule='0 8 * * *',
    catchup=True
) as dag:

    transfer_soil_data = SQLExecuteQueryOperator(
        task_id='transfer_soil_data',
        conn_id='raw_zone_conn',
        sql=queries.soil_data
    )
    
    transfer_meteo_data = SQLExecuteQueryOperator(
        task_id='transfer_meteo_data',
        conn_id='raw_zone_conn',
        sql=queries.meteo_data
    )
    
    transfer_agro_meteo_data = SQLExecuteQueryOperator(
        task_id='transfer_agro_meteo_data',
        conn_id='raw_zone_conn',
        sql=queries.agro_meteo_data
    )
    
    transfer_soil_data >> transfer_meteo_data >> transfer_agro_meteo_data
    

    
    