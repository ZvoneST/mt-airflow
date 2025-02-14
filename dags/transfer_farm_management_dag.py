import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from include.transfer_farm_management_queries import farm_management_transfers

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

conn = BaseHook.get_connection('exploration_zone_conn')

default_args = {
    'owner': 'admin',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

queries = farm_management_transfers(user=conn.login, password=conn.password)

with DAG(
    dag_id='transfer_farm_management',
    default_args=default_args,
    schedule='0 1 * * *',
    catchup=False
) as dag:

    agent_types = SQLExecuteQueryOperator(
        task_id='agent_types',
        conn_id='exploration_zone_conn',
        sql=queries.agent_types
    )
    
    agents = SQLExecuteQueryOperator(
        task_id='agents',
        conn_id='exploration_zone_conn',
        sql=queries.agents
    )
    
    agro_organizations = SQLExecuteQueryOperator(
        task_id='agro_organizations',
        conn_id='exploration_zone_conn',
        sql=queries.agro_organizations
    )
    
    agrotechnical_operation_groups = SQLExecuteQueryOperator(
        task_id='agrotechnical_operation_groups',
        conn_id='exploration_zone_conn',
        sql=queries.agrotechnical_operation_groups
    )
    
    agrotechnical_operations = SQLExecuteQueryOperator(
        task_id='agrotechnical_operations',
        conn_id='exploration_zone_conn',
        sql=queries.agrotechnical_operations
    )
    
    crops = SQLExecuteQueryOperator(
        task_id='crops',
        conn_id='exploration_zone_conn',
        sql=queries.crops
    )
    
    varieties = SQLExecuteQueryOperator(
        task_id='varieties',
        conn_id='exploration_zone_conn',
        sql=queries.varieties
    )
    
    measurement_units = SQLExecuteQueryOperator(
        task_id='measurement_units',
        conn_id='exploration_zone_conn',
        sql=queries.measurement_units
    )
    
    meteo_locations = SQLExecuteQueryOperator(
        task_id='meteo_locations',
        conn_id='exploration_zone_conn',
        sql=queries.meteo_locations
    )
    
    production_types = SQLExecuteQueryOperator(
        task_id='production_types',
        conn_id='exploration_zone_conn',
        sql=queries.production_types
    )
    
    seasons = SQLExecuteQueryOperator(
        task_id='seasons',
        conn_id='exploration_zone_conn',
        sql=queries.seasons
    )
    
    fields = SQLExecuteQueryOperator(
        task_id='fields',
        conn_id='exploration_zone_conn',
        sql=queries.fields
    )
    
    field_tasks = SQLExecuteQueryOperator(
        task_id='field_tasks',
        conn_id='exploration_zone_conn',
        sql=queries.field_tasks
    )
    
    (agent_types >> agents >> agro_organizations >> \
    agrotechnical_operation_groups >> agrotechnical_operations) >> \
    (crops >> varieties >> measurement_units >> meteo_locations >> \
    production_types >> seasons) >> fields >> field_tasks
    
    