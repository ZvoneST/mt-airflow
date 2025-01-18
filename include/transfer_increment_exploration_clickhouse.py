import logging
from typing import List
from dataclasses import dataclass
import pandas as pd
from sqlalchemy import create_engine
import clickhouse_connect as chc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def select_field_tasks_query(filter_date: str):
   return f'''
        SELECT 
            field_task_id, agrotehnical_operation_id, agro_organization_id, field_id, crop_id, variety_id, production_type_id, 
            agent_id, season_id, due_date, measurement_unit_id, quantity_ha, total_quantity, created_on
        FROM 
            farm_management.field_tasks
        WHERE 
            created_on > {filter_date};'''

def select_meteo_data_query(filter_date: str):
    return f'''
        SELECT 
            meteo_location_id, air_temperature, humidity, 
            precipitation, pressure, meteo_time, created_on
        FROM 
            external_data.hourly_meteo_data
        WHERE 
            created_on > {filter_date};'''
    
def select_agro_meteo_data_query(filter_date: str): 
    return f'''
        SELECT 
            meteo_location_id, soil_moisture, soil_moisture_10cm, 
            soil_temperature, soil_temperature_10cm, meteo_time, created_on
        FROM 
            external_data.hourly_agro_meteo_data
        WHERE 
            created_on > {filter_date};'''


class CKHSTransferIncrementData:
    
    def __init__(self, pg_conn: str, ckhs_config: dict):
        self.pg_conn = pg_conn
        self.ckhs_config = ckhs_config
        
    def _fetch_latest_created_on(self, database: str, table):
        try:
            client = chc.get_client(**self.ckhs_config)
            query = f'SELECT MAX(created_on) AS latest_date FROM {database}.{table}'
            result = client.command(query)
            latest_date = result if result else None
            logger.info(f'Latest `created_on` in ClickHouse: {latest_date}')
            return latest_date
        except Exception as e:
            logger.error('Error fetching latest `created_on` from ClickHouse:', e)
            return None
        
    def _fetch_data_from_postgres(self, latest_date, table: str):
        try:
            engine = create_engine(self.pg_conn)
            
            if table == 'field_tasks':
                query = select_field_tasks_query(filter_date=latest_date)
            elif table == 'meteo_data':
                query = select_meteo_data_query(filter_date=latest_date)
            else:
                query = select_agro_meteo_data_query(filter_date=latest_date)
                
            df = pd.read_sql_query(query, engine)
            
            logger.info(f'Fetched {len(df)} rows from PostgreSQL.')
            return df
        except Exception as e:
            logger.error('Error fetching data from PostgreSQL:', e)
            return None
        
    def _insert_data_to_clickhouse(self, df: pd.DataFrame):
        try:
            client = chc.get_client(**self.ckhs_config)
            column_names = df.columns.tolist()
            data = df.values.tolist()
            client.insert('field_tasks', data, column_names=column_names)
            logger.info(f'{len(data)} rows inserted into ClickHouse.')
        except Exception as e:
            logger.error('Error inserting data into ClickHouse:', e)
    
    def transfer_field_tasks(self, database: str, table: str):
        latest_date = self._fetch_latest_created_on(database=database, table=table)
        if latest_date is None:
            logger.info('No existing data in ClickHouse. Fetching all data from PostgreSQL.')
            latest_date = '1970-01-01 00:00:00'
        
        df = self._fetch_data_from_postgres(latest_date=latest_date, table=table)
        
        if df is not None and not df.empty:
            self._insert_data_to_clickhouse(df)
        else:
            logger.warning('No new data to insert.')
        
    
    
    











    
    