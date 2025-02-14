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
            field_task_id, agrotechnical_operation_id, agro_organization_id, field_id, crop_id, variety_id, production_type_id, 
            agent_id, season_id, due_date, measurement_unit_id, quantity_ha, total_quantity
        FROM 
            farm_management.field_tasks
        WHERE 
            created_on > '{filter_date}';'''

def select_meteo_data_query(filter_date: str):
    return f'''
        SELECT 
            meteo_location_id, air_temperature, humidity, 
            precipitation, pressure, meteo_time
        FROM 
            external_data.hourly_meteo_data
        WHERE 
            created_on > '{filter_date}';'''
    
def select_agro_meteo_data_query(filter_date: str): 
    return f'''
        SELECT 
            meteo_location_id, soil_moisture, soil_moisture_10cm, 
            soil_temperature, soil_temperature_10cm, meteo_time
        FROM 
            external_data.hourly_agro_meteo_data
        WHERE 
            created_on > '{filter_date}';'''
            
def select_vegetation_indices_data_query(filter_date: str): 
    return f'''
        SELECT 
            vegetation_index_id, field_id, interval_from, interval_to, 
            indices_ndvi_min, indices_ndvi_max, indices_ndvi_mean, indices_ndvi_stdev, 
            indices_savi_min, indices_savi_max, indices_savi_mean, indices_savi_stdev, 
            indices_wdrvi_min, indices_wdrvi_max, indices_wdrvi_mean, indices_wdrvi_stdev, 
            sample_count, no_data_count, created_on
        FROM external_data.vegetation_indices
        WHERE created_on > '{filter_date}';
    '''


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
            logger.error('Error fetching latest `created_on` from ClickHouse:', exc_info=True)
            return None
        
    def _fetch_data_from_postgres(self, latest_date, table: str):
        try:         
            if table == 'field_tasks':
                source_query = select_field_tasks_query(filter_date=latest_date)
            elif table == 'hourly_meteo_data':
                source_query = select_meteo_data_query(filter_date=latest_date)
            elif table == 'hourly_agro_meteo_data':
                source_query = select_agro_meteo_data_query(filter_date=latest_date)
            else:
                source_query = select_vegetation_indices_data_query(filter_date=latest_date)
                
            with self.pg_conn.cursor() as cursor:
                cursor.execute(source_query)
                rows = cursor.fetchall()

                column_names = [desc[0] for desc in cursor.description]

            df = pd.DataFrame(rows, columns=column_names)

            logger.info(f'Fetched {len(df)} rows from PostgreSQL.')
            return df
        except Exception as e:
            logger.error('Error fetching data from PostgreSQL:', exc_info=True)
            return None
        finally:
            if 'connection' in locals() and self.pg_conn:
                self.pg_conn.close()
        
    def _insert_data_to_clickhouse(self, df: pd.DataFrame, table: str):
        try:
            client = chc.get_client(**self.ckhs_config)
            column_names = df.columns.tolist()
            data = df.values.tolist()
            client.insert(table=table, data=data, column_names=column_names)
            logger.info(f'{len(data)} rows inserted into ClickHouse.')
        except Exception as e:
            logger.error('Error inserting data into ClickHouse:', exc_info=True)
    
    def transfer_increment_data(self, database: str, table: str):
        latest_date = self._fetch_latest_created_on(database=database, table=table)
        if latest_date is None:
            logger.info('No existing data in ClickHouse. Fetching all data from PostgreSQL.')
            latest_date = '1970-01-01 00:00:00'
        
        df = self._fetch_data_from_postgres(latest_date=latest_date, table=table)
        
        if df is not None and not df.empty:
            self._insert_data_to_clickhouse(df=df, table=table)
        else:
            logger.warning('No new data to insert.')


class CallableCKHSTransferIncrementData():
    
    def __init__(self, pg_conn: str, ckhs_config: dict, database: str):
        self.transfer_data = CKHSTransferIncrementData(pg_conn=pg_conn, 
                                                       ckhs_config=ckhs_config)
        self.database = database
        
    def transfer_field_tasks(self):
        self.transfer_data.transfer_increment_data(
            database=self.database,
            table='field_tasks'
        )

    def transfer_meteo_data(self):
        self.transfer_data.transfer_increment_data(
            database=self.database,
            table='hourly_meteo_data'
        )

    def transfer_agro_meteo_data(self):
        self.transfer_data.transfer_increment_data(
            database=self.database,
            table='hourly_agro_meteo_data'
        )
        
    def transfer_vegetation_indices_data(self):
        self.transfer_data.transfer_increment_data(
            database=self.database,
            table='vegetation_indices'
        )