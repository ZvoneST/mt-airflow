import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from include.transfer_raw_exploration_queries import transfer_raw_exploration_queries

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

source_conn = BaseHook.get_connection('raw_zone_conn')

queries = transfer_raw_exploration_queries(user=source_conn.login, 
                                           password=source_conn.password)

with DAG(
    dag_id='transfer_raw_exploration',
    default_args=default_args,
    schedule='0 9 * * *',
    catchup=True
) as dag:

    transfer_raw_exploration_soil = SQLExecuteQueryOperator(
        task_id='transfer_raw_exploration_soil',
        conn_id='exploration_zone_conn',
        sql=queries.raw_exploration_soil
    )
    
    transfer_raw_exploration_meteo = SQLExecuteQueryOperator(
        task_id='transfer_raw_exploration_meteo',
        conn_id='exploration_zone_conn',
        sql=queries.raw_exploration_meteo
    )
    
    transfer_raw_exploration_agro_meteo = SQLExecuteQueryOperator(
        task_id='transfer_raw_exploration_agro_meteo',
        conn_id='exploration_zone_conn',
        sql=queries.raw_exploration_agro_meteo
    )
    
    transfer_raw_exploration_soil >> transfer_raw_exploration_meteo \
        >> transfer_raw_exploration_agro_meteo