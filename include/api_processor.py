import re
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


class APIProcessor:
    def __init__(self, source_conn, 
                 target_conn, 
                 fetch_query, 
                 insert_query, 
                 request_method,
                 api_key = None):
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
            lon, lat = float(match.group(1)), float(match.group(2))
            return lon, lat
        else:
            raise ValueError(f'Invalid WKT format: {wkt}')

    def store_raw_data(self, id, requested_data):
        try:
            with self.target_conn.cursor() as cursor:
                cursor.execute(self.insert_query, (id, json.dumps(requested_data)))
                self.target_conn.commit()
        except Exception as e:
            self.target_conn.rollback()
            raise e

    def process_data(self):
        try:
            data = self.fetch_ids()

            for id, wkt in data:
                try:
                    lon, lat = self.parse_wkt_coordinates(wkt=wkt)
                    logger.info(f'Processing id {id} with coordinates ({lon}, {lat})...')
                    if self.api_key is not None:
                        requested_data = self.request_method(lon, lat, self.api_key)
                    else:
                        requested_data = self.request_method(lon, lat)

                    self.store_raw_data(id=id, requested_data=requested_data)
                    logger.info(f'Successfully processed id {id}.')
                except Exception as e:
                    logger.error(f'Failed to process id {id}: {e}')
        finally:
            self.source_conn.close()
            self.target_conn.close()