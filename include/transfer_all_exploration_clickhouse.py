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


# @dataclass
# class StagingCKHColumnNames:
#     agent_types_cn: List[str] = ['agent_type_id', 'agent_type_name']
#     agents_cn: List[str] = ['agent_id', 'agent_type_id', 'agent_name']
#     agro_organizations_cn: List[str] = ['agro_organization_id', 'agro_organization_name']
#     agrotehincal_operation_groups_cn: List[str] = ['agrotehnical_operation_group_id', 'agrotehnical_operation_group_name']
#     agrotehincal_operations_cn: List[str] = ['agrotehnical_operation_id', 'agrotehnical_operation_name', 'agrotehnical_operation_group_id']
#     crops_cn: List[str] = ['crop_id', 'crop_name']
#     fields_cn: List[str] = ['field_id', 'agro_organization_id', 'field_name', 'area_ha', 'field_polygon_wkt', 'centroid_coordinates_wkt', 'meteo_location_id']
#     field_tasks
#     measurement_units_cn: List[str] = ['measurement_unit_id', 'measurement_unit_label']
#     meteo_locations_cn: List[str] = ['meteo_location_id', 'meteo_location_name', 'meteo_coordinates_wkt']
#     production_types_cn: List[str] = ['production_type_id', 'production_type_name']
#     seasons_cn: List[str] = ['season_id', 'season', 'season_label']
#     varieties_cn: List[str] = ['variety_id', 'crop_id', 'variety_name']

def postgres_conn_string(user: str, password: str, host: str, port: str, database: str) -> str:
    return f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}'

def clickhouse_conn_config(host: str, port: int, username: str, password: str, database: str, secure: bool) -> dict:
    return {'host': host,
            'port': port,
            'username': username,
            'password': password,
            'database': database,
            'secure': secure}

@dataclass
class SelectAllQueries():
    agent_types: str = 'SELECT agent_type_id, agent_type_name FROM farm_management.agent_types;'
    agents: str = 'SELECT agent_id, agent_type_id, agent_name FROM farm_management.agents;'
    agro_organizations: str = 'SELECT agro_organization_id, agro_organization_name FROM farm_management.agro_organizations;'
    agrotehincal_operation_groups: str = 'SELECT agrotehnical_operation_group_id, agrotehnical_operation_group_name FROM farm_management.agrotehincal_operation_groups;'
    agrotehincal_operations: str = 'SELECT agrotehnical_operation_id, agrotehnical_operation_name, agrotehnical_operation_group_id FROM farm_management.agrotehincal_operations;'
    crops: str = 'SELECT crop_id, crop_name FROM farm_management.crops;'
    fields: str = 'SELECT field_id, agro_organization_id, field_name, area_ha, field_polygon_wkt, centroid_coordinates_wkt, meteo_location_id FROM farm_management.fields;'
    measurement_units: str = 'SELECT measurement_unit_id, measurement_unit_label FROM farm_management.measurement_units;'
    meteo_locations: str = 'SELECT meteo_location_id, meteo_location_name, meteo_coordinates_wkt FROM farm_management.meteo_locations;'
    production_types: str = 'SELECT production_type_id, production_type_name FROM farm_management.production_types;'
    seasons: str = 'SELECT season_id, season, season_label FROM farm_management.seasons;'
    varieties: str = 'SELECT variety_id, crop_id, variety_name FROM farm_management.varieties;'
    soil_data: str = 'SELECT field_id, soil_depth, bdod, clay, silt, sand, nitrogen, soc, phh2o, cec, created_on FROM external_data.soil_data'

class CKHSTransferAllData:
    
    def __init__(self, pg_conn: str, ckhs_conn: dict):
        self.pg_conn = pg_conn
        self.ckhs_conn = ckhs_conn
    
    def _extract_data_from_postgres(self, source_query: str) -> pd.DataFrame:
        try:
            engine = create_engine(self.pg_conn)
            df = pd.read_sql_query(source_query, engine)
            logger.info(f'Extracted {len(df)} rows from PostgreSQL.')
            return df
        except Exception as e:
            logger.error('Error reading data from Postgres:', e)
            return None
    
    def _load_data_to_clickhouse(self, df: pd.DataFrame, database: str, table: str):
        try:
            client = chc.get_client(self.ckhs_conn)
            
            client.command(f'TRUNCATE TABLE {database}.{table};')
            print(f'Existing data in ClickHouse table {table} has been deleted.')
            
            column_names = df.columns.tolist()
            data = df.values.tolist()
                    
            client.insert(database=database, table=table, data=data, column_names=column_names)
            logger.info(f'{len(data)} rows inserted into ClickHouse.')
        except Exception as e:
            logger.error(f'Error writing data to ClickHouse table {table}:', e)
    
    def transfer_data(self, data_query: str, dest_database: str, dest_table: str):
        df = self._extract_data_from_postgres(source_query=data_query)
        if df is not None and not df.empty:
            self._load_data_to_clickhouse(df=df, database=dest_database, table=dest_table)
        else:
            logger.warning('No data to transfer.')
    











    
    