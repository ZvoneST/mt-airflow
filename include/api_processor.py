import re
import json
import logging
from psycopg2 import extras
from requests.exceptions import RequestException

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class APIProcessor:
    
    def __init__(self, source_conn, target_conn, fetch_query, insert_query, request_method, api_key=None):
        self.source_conn = source_conn
        self.target_conn = target_conn
        self.fetch_query = fetch_query
        self.insert_query = insert_query
        self.request_method = request_method
        self.api_key = api_key

    def fetch_ids(self):
        with self.source_conn.cursor() as cursor:
            cursor.execute(self.fetch_query)
            return cursor.fetchall()

    @staticmethod
    def parse_wkt_coordinates(wkt):
        match = re.match(r'POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)', wkt)
        if match:
            return float(match.group(1)), float(match.group(2))
        else:
            raise ValueError(f'Invalid WKT format: {wkt}')

    def store_raw_data(self, records):
        try:
            with self.target_conn.cursor() as cursor:
                extras.execute_batch(cursor, self.insert_query, records)
                self.target_conn.commit()
                logger.info(f'Successfully inserted {len(records)} records.')
        except Exception as e:
            self.target_conn.rollback()
            logger.error(f'Failed to store records: {e}')
            raise e

    def process_data(self):
        try:
            data = self.fetch_ids()
            batch_records = []

            for id, wkt in data:
                try:
                    lon, lat = self.parse_wkt_coordinates(wkt=wkt)
                    logger.info(f'Processing id {id} with coordinates ({lon}, {lat})...')

                    if self.api_key:
                        requested_data = self.request_method(lon, lat, self.api_key)
                    else:
                        requested_data = self.request_method(lon, lat)

                    batch_records.append((id, json.dumps(requested_data)))
                    
                    if len(batch_records) >= 100:
                        self.store_raw_data(batch_records)
                        batch_records = []

                except RequestException as req_err:
                    logger.error(f'API request failed for id {id}: {req_err}')
                except ValueError as val_err:
                    logger.error(f'Data parsing error for id {id}: {val_err}')
                except Exception as e:
                    logger.error(f'Unexpected error for id {id}: {e}')

            if batch_records:
                self.store_raw_data(batch_records)

        except Exception as e:
            logger.error(f'Error during processing: {e}')
        finally:
            self.source_conn.close()
            self.target_conn.close()