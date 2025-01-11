import re
import json


class APIProcessor:
    def __init__(self, source_conn, target_conn, fetch_query, insert_query, request_method):
        self.source_conn = source_conn
        self.target_conn = target_conn
        self.fetch_query = fetch_query
        self.insert_query = insert_query
        self.request_method = request_method

    def fetch_field_ids(self):
        """Fetch field IDs and WKT coordinates from the source database."""
        with self.source_conn.cursor() as cursor:
            cursor.execute(self.fetch_query)
            return cursor.fetchall()

    @staticmethod
    def parse_wkt_coordinates(wkt):
        """Extract longitude and latitude from WKT point."""
        match = re.match(r"POINT\s*\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", wkt)
        if match:
            lon, lat = float(match.group(1)), float(match.group(2))
            return lon, lat
        else:
            raise ValueError(f"Invalid WKT format: {wkt}")

    def store_raw_data(self, field_id, requested_data):
        """Store the fetched SoilGrids data into the target database."""
        try:
            with self.target_conn.cursor() as cursor:
                cursor.execute(self.insert_query, (field_id, json.dumps(requested_data)))
                self.target_conn.commit()
        except Exception as e:
            self.target_conn.rollback()
            raise e

    def process_data(self):
        """Fetch field IDs, send SoilGrids requests, and store the data."""
        try:
            field_data = self.fetch_field_ids()

            for field_id, wkt in field_data:
                try:
                    lon, lat = self.parse_wkt_coordinates(wkt=wkt)
                    print(f"Processing field_id {field_id} with coordinates ({lon}, {lat})...")

                    requested_data = self.request_method(lon, lat)

                    self.store_raw_data(field_id=field_id, requested_data=requested_data)
                    print(f"Successfully processed field_id {field_id}.")
                except Exception as e:
                    print(f"Failed to process field_id {field_id}: {e}")
        finally:
            self.source_conn.close()
            self.target_conn.close()