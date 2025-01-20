import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from include.transfer_all_exploration_clickhouse import postgres_conn, clickhouse_conn_config, \
    CallableCKHSTransferAllData
from include.transfer_increment_exploration_clickhouse import CallableCKHSTransferIncrementData
from datetime import datetime

default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

source_conn = BaseHook.get_connection('exploration_zone_conn')
target_conn = BaseHook.get_connection('staging_clickhouse_conn')

pg_conn = postgres_conn(user=source_conn.login,
                        password=source_conn.password,
                        host=source_conn.host,
                        port=source_conn.port,
                        database=source_conn.schema)

ckhs_config = clickhouse_conn_config(user=target_conn.login,
                                     password=target_conn.password,
                                     host=target_conn.host,
                                     port=target_conn.port,
                                     database=target_conn.schema)

transfer_all_process = CallableCKHSTransferAllData(pg_conn=pg_conn, ckhs_config=ckhs_config)

transfer_increment_process = CallableCKHSTransferIncrementData(pg_conn=pg_conn, ckhs_config=ckhs_config)

with DAG(
    dag_id='transfer_exploration_staging',
    default_args=default_args,
    schedule='0 11 * * *',
    catchup=True
) as dag:
    
    staging_agent_types = PythonOperator(
        task_id='staging_agent_types',
        python_callable=transfer_all_process.transfer_agent_types
    )
    
    staging_agents = PythonOperator(
        task_id='staging_agents',
        python_callable=transfer_all_process.transfer_agents
    )

    staging_agro_organizations = PythonOperator(
        task_id='agro_organizations',
        python_callable=transfer_all_process.transfer_agro_organizations
    )

    staging_agrotehincal_operation_groups = PythonOperator(
        task_id='staging_agrotehincal_operation_groups',
        python_callable=transfer_all_process.transfer_agrotehincal_operation_groups
    )

    staging_agrotehincal_operations = PythonOperator(
        task_id='staging_agrotehincal_operations',
        python_callable=transfer_all_process.transfer_agrotehincal_operations
    )
    
    staging_crops = PythonOperator(
        task_id='staging_crops',
        python_callable=transfer_all_process.transfer_crops
    )
    
    staging_fields = PythonOperator(
        task_id='staging_fields',
        python_callable=transfer_all_process.transfer_fields
    )
    
    staging_measurement_units = PythonOperator(
        task_id='staging_measurement_units',
        python_callable=transfer_all_process.transfer_measurement_units
    )
    
    staging_meteo_locations = PythonOperator(
        task_id='staging_meteo_locations',
        python_callable=transfer_all_process.transfer_meteo_locations
    )
    
    staging_production_types = PythonOperator(
        task_id='staging_production_types',
        python_callable=transfer_all_process.transfer_production_types
    )
    
    staging_seasons = PythonOperator(
        task_id='staging_seasons',
        python_callable=transfer_all_process.transfer_seasons
    )
    
    staging_varieties = PythonOperator(
        task_id='staging_varieties',
        python_callable=transfer_all_process.transfer_varieties
    )
    
    staging_soil_data = PythonOperator(
        task_id='staging_soil_data',
        python_callable=transfer_all_process.transfer_soil_data
    )
    
    staging_field_tasks = PythonOperator(
        task_id='staging_field_tasks',
        python_callable=transfer_increment_process.transfer_field_tasks
    )
    
    staging_meteo_data = PythonOperator(
        task_id='staging_meteo_data',
        python_callable=transfer_increment_process.transfer_meteo_data
    )
    
    staging_agro_meteo = PythonOperator(
        task_id='staging_agro_meteo',
        python_callable=transfer_increment_process.transfer_agro_meteo_data
    )
    
    staging_vegetation_indices = PythonOperator(
        task_id='staging_vegetation_indices',
        python_callable=transfer_increment_process.transfer_vegetation_indices_data
    )
    
    
    
    
    staging_agent_types >> staging_agents >> staging_agro_organizations >> \
    staging_agrotehincal_operation_groups >> staging_agrotehincal_operations >> \
    staging_crops >> staging_fields >> staging_measurement_units >> \
    staging_meteo_locations >> staging_production_types >> \
    staging_seasons >> staging_varieties >> staging_soil_data >> \
    staging_field_tasks >> staging_meteo_data >> staging_agro_meteo >> \
    staging_vegetation_indices
    