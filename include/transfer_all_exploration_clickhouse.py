import logging
from typing import List
from dataclasses import dataclass
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import clickhouse_connect as chc

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def postgres_conn(user: str, password: str, host: str, port: str, database: str) -> str:
    return psycopg2.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
    )


def clickhouse_conn_config(host: str, port: int, user: str, password: str, database: str, secure: bool = True) -> dict:
    return {'host': host,
            'port': port,
            'username': user,
            'password': password,
            'database': database,
            'secure': secure}

@dataclass
class StagingTables:
    agent_types: str = 'agent_types'
    agents: str = 'agents'
    agro_organizations: str = 'agro_organizations'
    agrotehincal_operation_groups: str = 'agrotehnical_operation_groups'
    agrotehincal_operations: str = 'agrotehnical_operations'
    crops: str = 'crops'
    fields: str = 'fields'
    measurement_units: str = 'measurement_units'
    meteo_locations: str = 'meteo_locations'
    production_types: str = 'production_types'
    seasons: str = 'seasons'
    varieties: str = 'varieties'
    soil_data: str = 'soil_data'
    

@dataclass
class SelectAllQueries():
    agent_types: str = 'SELECT agent_type_id, agent_type_name FROM farm_management.agent_types;'
    agents: str = 'SELECT agent_id, agent_type_id, agent_name FROM farm_management.agents;'
    agro_organizations: str = 'SELECT agro_organization_id, agro_organization_name FROM farm_management.agro_organizations;'
    agrotehincal_operation_groups: str = 'SELECT agrotehnical_operation_group_id, agrotehnical_operation_group_name FROM farm_management.agrotehnical_operation_groups;'
    agrotehincal_operations: str = 'SELECT agrotehnical_operation_id, agrotehnical_operation_name, agrotehnical_operation_group_id FROM farm_management.agrotehnical_operations;'
    crops: str = 'SELECT crop_id, crop_name FROM farm_management.crops;'
    fields: str = 'SELECT field_id, agro_organization_id, field_name, area_ha, field_polygon_wkt, centroid_coordinates_wkt, meteo_location_id FROM farm_management.fields;'
    measurement_units: str = 'SELECT measurement_unit_id, measurement_unit_label FROM farm_management.measurement_units;'
    meteo_locations: str = 'SELECT meteo_location_id, meteo_location_name, meteo_coordinates_wkt FROM farm_management.meteo_locations;'
    production_types: str = 'SELECT production_type_id, production_type_name FROM farm_management.production_types;'
    seasons: str = 'SELECT season_id, season, season_label FROM farm_management.seasons;'
    varieties: str = 'SELECT variety_id, crop_id, variety_name FROM farm_management.varieties;'
    soil_data: str = 'SELECT field_id, soil_depth, bdod, clay, silt, sand, nitrogen, soc, phh2o, cec, created_on FROM external_data.soil_data'


class CKHSTransferAllData:
    
    def __init__(self, pg_conn: str, ckhs_config: dict):
        self.pg_conn = pg_conn
        self.ckhs_config = ckhs_config
    
    def _extract_data_from_postgres(self, source_query: str) -> pd.DataFrame:
        try:
            with self.pg_conn.cursor() as cursor:
                cursor.execute(source_query)
                
                rows = cursor.fetchall()
                
                column_names = [desc[0] for desc in cursor.description]
                
                df = pd.DataFrame(rows, columns=column_names)
            
            logger.info(f'Extracted {len(df)} rows from PostgreSQL.')
            return df
        
        except Exception as e:
            logger.error('Error reading data from Postgres:', exc_info=True)
            return None
        finally:
            if 'connection' in locals() and self.pg_conn:
                self.pg_conn.close()

    def _load_data_to_clickhouse(self, df: pd.DataFrame, database: str, table: str):
        try:
            client = chc.get_client(**self.ckhs_config)
            
            client.command(f'TRUNCATE TABLE {database}.{table};')
            print(f'Existing data in ClickHouse table {table} has been deleted.')
            
            column_names = df.columns.tolist()
            data = df.values.tolist()
                    
            client.insert(database=database, table=table, data=data, column_names=column_names)
            logger.info(f'{len(data)} rows inserted into ClickHouse.')
        except Exception as e:
            logger.error(f'Error writing data to ClickHouse table {table}:', exc_info=True)
    
    def transfer_data(self, data_query: str, dest_database: str, dest_table: str):
        df = self._extract_data_from_postgres(source_query=data_query)
        if df is not None and not df.empty:
            self._load_data_to_clickhouse(df=df, database=dest_database, table=dest_table)
        else:
            logger.warning('No data to transfer.')
            

class CallableCKHSTransferAllData():
    
    def __init__(self, pg_conn: str, ckhs_config: dict, database: str):
        self.transfer_data = CKHSTransferAllData(pg_conn=pg_conn, 
                                                 ckhs_config=ckhs_config)
        self.database = database
        self.stg_tables = StagingTables()
        self.queries = SelectAllQueries()
        
    def transfer_agent_types(self):
        self.transfer_data.transfer_data(
            data_query=self.queries.agent_types,
            dest_database=self.database,
            dest_table=self.stg_tables.agent_types
        )
    
    def transfer_agents(self):
        self.transfer_data.transfer_data(
            data_query=self.queries.agents,
            dest_database=self.database,
            dest_table=self.stg_tables.agents
        )
        
    def transfer_agro_organizations(self):
        self.transfer_data.transfer_data(
            data_query=self.queries.agro_organizations,
            dest_database=self.database,
            dest_table=self.stg_tables.agro_organizations
        )
        
    def transfer_agrotehincal_operation_groups(self):
        self.transfer_data.transfer_data(
            data_query=self.queries.agrotehincal_operation_groups,
            dest_database=self.database,
            dest_table=self.stg_tables.agrotehincal_operation_groups
        )
        
    def transfer_agrotehincal_operations(self):
        self.transfer_data.transfer_data(
            data_query=self.queries.agrotehincal_operations,
            dest_database=self.database,
            dest_table=self.stg_tables.agrotehincal_operations
        )
    
    def transfer_crops(self):
        self.transfer_data.transfer_data(
            data_query=self.queries.crops,
            dest_database=self.database,
            dest_table=self.stg_tables.crops
        )

    def transfer_fields(self):
        self.transfer_data.transfer_data(
            data_query=self.queries.fields,
            dest_database=self.database,
            dest_table=self.stg_tables.fields
        )
    
    def transfer_measurement_units(self):
        self.transfer_data.transfer_data(
            data_query=self.queries.measurement_units,
            dest_database=self.database,
            dest_table=self.stg_tables.measurement_units
        )

    def transfer_meteo_locations(self):
        self.transfer_data.transfer_data(
            data_query=self.queries.meteo_locations,
            dest_database=self.database,
            dest_table=self.stg_tables.meteo_locations
        )

    def transfer_production_types(self):
        self.transfer_data.transfer_data(
            data_query=self.queries.production_types,
            dest_database=self.database,
            dest_table=self.stg_tables.production_types
        )
    
    def transfer_seasons(self):
        self.transfer_data.transfer_data(
            data_query=self.queries.seasons,
            dest_database=self.database,
            dest_table=self.stg_tables.seasons
        )

    def transfer_varieties(self):
        self.transfer_data.transfer_data(
            data_query=self.queries.varieties,
            dest_database=self.database,
            dest_table=self.stg_tables.varieties
        )
    
    def transfer_soil_data(self):
        self.transfer_data.transfer_data(
            data_query=self.queries.soil_data,
            dest_database=self.database,
            dest_table=self.stg_tables.soil_data
        )
