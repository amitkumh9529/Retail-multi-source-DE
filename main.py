"""
RetailPulse Pipeline Orchestrator & CLI Runner.

Provides a dry-run local verification mode of the entire extraction and watermark suite
without requiring active Snowflake or AWS credentials.
"""
import argparse
import logging
import os
import sys
from dotenv import load_dotenv

load_dotenv()

from ingestion.ecommerce_api.extractor import EcommerceOrdersExtractor
from ingestion.pos_database.extractor import PosDatabaseExtractor
from ingestion.threepl_files.processor import ThreePLFileProcessor
from ingestion.watermark import WatermarkManager
from ingestion.s3_uploader import DataLakeWriter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("retailpulse.main")


def run_pipeline(max_api_pages=None):
    logger.info("======================================================================")
    logger.info("Starting RetailPulse Batch ELT Pipeline (Direct S3 Ingestion)")
    logger.info("======================================================================")

    bucket = os.getenv("AWS_S3_BUCKET_RAW", "retailpulse-s3")
    logger.info(f"Target AWS S3 Bucket: {bucket}")

    watermark_mgr = WatermarkManager(local_path="./.watermarks.db")
    writer = DataLakeWriter(bucket_name=bucket, local_lake_root="./data_lake_storage")

    # 1. Ingest 3PL Files
    logger.info("\n--- [Step 1/3] Processing 3PL Warehouse & Logistics CSV Drops ---")
    threepl_proc = ThreePLFileProcessor(writer=writer)
    threepl_res = threepl_proc.extract()
    logger.info(f"3PL Result: {threepl_res.records_extracted} rows extracted, {threepl_res.errors_encountered} quarantined.")

    # 2. Ingest POS Database
    logger.info("\n--- [Step 2/3] Extracting POS PostgreSQL Database ---")
    pos_dsn = os.getenv("SUPABASE_DB_URL")
    if pos_dsn:
        pos_extractor = PosDatabaseExtractor(dsn=pos_dsn, writer=writer, watermark_mgr=watermark_mgr)
        try:
            dim_res = pos_extractor.extract_dimensions()
            logger.info(f"POS Dimensions: {dim_res.records_extracted} records extracted.")
            txn_res = pos_extractor.extract()
            logger.info(f"POS Transactions: {txn_res.records_extracted} records extracted.")
        except Exception as e:
            logger.warning(f"POS extraction skipped due to connection: {e}")
    else:
        logger.warning("SUPABASE_DB_URL not configured. Skipping live POS extract.")

    # 3. Ingest E-commerce API
    logger.info("\n--- [Step 3/3] Extracting E-Commerce Orders REST API ---")
    ecomm_extractor = EcommerceOrdersExtractor(writer=writer, watermark_mgr=watermark_mgr)
    try:
        ecomm_res = ecomm_extractor.extract(max_pages=max_api_pages)
        logger.info(f"E-commerce Result: {ecomm_res.records_extracted} records extracted, {ecomm_res.errors_encountered} quarantined.")
    except Exception as e:
        logger.warning(f"E-commerce API extraction note: {e} (Ensure mock_api/app.py is running)")

    logger.info("\n======================================================================")
    logger.info(f"Pipeline Execution Completed! Parquet files landed in S3: s3://{bucket}/")
    logger.info("======================================================================")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RetailPulse ELT Pipeline Runner")
    parser.add_argument("--pages", type=int, default=10, help="Max API pages to extract (default: 10)")
    args = parser.parse_args()

    run_pipeline(max_api_pages=args.pages)
