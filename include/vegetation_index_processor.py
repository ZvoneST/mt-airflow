import os
import logging
import json
import rasterio
import numpy as np
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


class VegetationIndexProcessor:
    
    def __init__(self, db_connection):
        self.db_connection = db_connection

    @staticmethod
    def calculate_indices(b4, b8):
        np.seterr(divide="ignore", invalid="ignore")
        ndvi = (b8 - b4) / (b8 + b4)
        savi = ((b8 - b4) * 1.5) / (b8 + b4 + 0.5)
        alpha = 0.1
        wdrvi = (alpha * b8 - b4) / (alpha * b8 + b4)
        logger.info("Calculated vegetation indices.")
        return ndvi, savi, wdrvi

    @staticmethod
    def aggregate_values(array):
        return {
            "mean": np.nanmean(array),
            "min": np.nanmin(array),
            "max": np.nanmax(array),
            "std": np.nanstd(array),
        }

    @staticmethod
    def count_nulls_and_nonnulls(b4, b8):
        combined = np.stack([b4, b8], axis=0)
        return {
            "nonnull_count": np.count_nonzero(~np.isnan(combined)),
            "null_count": np.count_nonzero(np.isnan(combined)),
        }

    def get_last_processed_date(self):
        """
        Fetches the maximum created_on timestamp from the vegetation_indices table.
        :return: The maximum created_on timestamp or None if no records exist.
        """
        query = "SELECT max(created_on) FROM external_data.vegetation_indices;"
        with self.db_connection.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
        return result[0] if result and result[0] else datetime.min

    def process_image(self, image_path, metadata_path):
        with rasterio.open(image_path) as src:
            b4 = src.read(1).astype("float32")
            b8 = src.read(2).astype("float32")

        ndvi, savi, wdrvi = self.calculate_indices(b4, b8)
        ndvi_stats = self.aggregate_values(ndvi)
        savi_stats = self.aggregate_values(savi)
        wdrvi_stats = self.aggregate_values(wdrvi)
        null_stats = self.count_nulls_and_nonnulls(b4, b8)

        with open(metadata_path, "r") as meta_file:
            metadata = json.load(meta_file)
            time_range = metadata["request"]["payload"]["input"]["data"][0][
                "dataFilter"
            ]["timeRange"]
            date_from = time_range["from"]
            date_to = time_range["to"]

        return date_from, date_to, ndvi_stats, savi_stats, wdrvi_stats, null_stats

    def fetch_data_from_db(self):
        last_processed_date = self.get_last_processed_date()
        query = f"""
        SELECT 
            field_id, 
            image_path, 
            response_meta_path, 
            created_on
        FROM external_data.satellite_metadata
        WHERE created_on > '{last_processed_date}';
        """
        with self.db_connection.cursor() as cur:
            cur.execute(query)
            records = cur.fetchall()
        return [
            {
                "field_id": row[0],
                "image_path": row[1],
                "response_meta_path": row[2],
                "created_on": row[3],
            }
            for row in records
        ]

    def store_results_in_db(self, record):
        for key, value in record.items():
            if isinstance(value, (np.float32, np.float64)):
                record[key] = float(value)
            elif isinstance(value, (np.int32, np.int64)):
                record[key] = int(value)
        
        query = """
        INSERT INTO external_data.vegetation_indices (
            field_id, interval_from, interval_to,
            indices_ndvi_min, indices_ndvi_max, indices_ndvi_mean, indices_ndvi_stdev,
            indices_savi_min, indices_savi_max, indices_savi_mean, indices_savi_stdev,
            indices_wdrvi_min, indices_wdrvi_max, indices_wdrvi_mean, indices_wdrvi_stdev,
            sample_count, no_data_count, created_on
        ) VALUES (
            %(field_id)s, %(interval_from)s, %(interval_to)s,
            %(ndvi_min)s, %(ndvi_max)s, %(ndvi_mean)s, %(ndvi_std)s,
            %(savi_min)s, %(savi_max)s, %(savi_mean)s, %(savi_std)s,
            %(wdrvi_min)s, %(wdrvi_max)s, %(wdrvi_mean)s, %(wdrvi_std)s,
            %(nonnull_count)s, %(null_count)s, %(created_on)s
        );
        """
        with self.db_connection.cursor() as cur:
            cur.execute(query, record)
        self.db_connection.commit()

    def process_images_iteratively(self, records):
        for record in records:
            field_id = record["field_id"]
            image_path = record["image_path"]
            metadata_path = record["response_meta_path"]

            if os.path.exists(image_path) and os.path.exists(metadata_path):
                logger.info(f"Processing image: {image_path}")
                date_from, date_to, ndvi_stats, savi_stats, wdrvi_stats, null_stats = (
                    self.process_image(image_path, metadata_path)
                )

                result = {
                    "field_id": field_id,
                    "interval_from": date_from,
                    "interval_to": date_to,
                    "ndvi_min": ndvi_stats["min"],
                    "ndvi_max": ndvi_stats["max"],
                    "ndvi_mean": ndvi_stats["mean"],
                    "ndvi_std": ndvi_stats["std"],
                    "savi_min": savi_stats["min"],
                    "savi_max": savi_stats["max"],
                    "savi_mean": savi_stats["mean"],
                    "savi_std": savi_stats["std"],
                    "wdrvi_min": wdrvi_stats["min"],
                    "wdrvi_max": wdrvi_stats["max"],
                    "wdrvi_mean": wdrvi_stats["mean"],
                    "wdrvi_std": wdrvi_stats["std"],
                    "nonnull_count": null_stats["nonnull_count"],
                    "null_count": null_stats["null_count"],
                    "created_on": datetime.now(),
                }

                yield result
            else:
                logger.warning(f"Missing file: {image_path} or {metadata_path}")

    def process(self):
        records = self.fetch_data_from_db()
        logger.info("Processing started.")
        for result in self.process_images_iteratively(records):
            self.store_results_in_db(result)
            logger.info(f"Stored result: {result}")
        self.db_connection.close()
        logger.info("Processing finished.")
