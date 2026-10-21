"""
RetailPulse POS Database Extractor (PostgreSQL / Supabase).

Performs incremental extraction on `pos_transactions` using the watermark window
and extracts reference dimensions (`stores`, `products`).
Converts chunked query results into columnar Parquet files landed in S3 / Data Lake.
"""
from datetime import datetime, timezone
import logging
import os
from typing import Optional
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from ingestion.base import BaseExtractor, ExtractionResult, with_retry
from ingestion.s3_uploader import DataLakeWriter
from ingestion.watermark import WatermarkManager

logger = logging.getLogger("retailpulse.pos.extractor")


class PosDatabaseExtractor(BaseExtractor):
    def __init__(
        self,
        dsn: Optional[str] = None,
        writer: Optional[DataLakeWriter] = None,
        watermark_mgr: Optional[WatermarkManager] = None,
        chunk_size: int = 10000,
    ):
        super().__init__(source_name="pos")
        self.dsn = dsn or os.getenv("SUPABASE_DB_URL")
        self.writer = writer or DataLakeWriter()
        self.watermark_mgr = watermark_mgr or WatermarkManager()
        self.chunk_size = chunk_size

    @with_retry(max_retries=3, initial_delay=2.0)
    def _get_connection(self):
        if not self.dsn:
            raise ValueError("SUPABASE_DB_URL is not set.")
        return psycopg2.connect(self.dsn, sslmode="require")

    def extract_dimensions(self) -> ExtractionResult:
        """Extract dimension tables: stores and products."""
        start_time = datetime.now(timezone.utc)
        date_str = start_time.strftime("%Y-%m-%d")
        s3_locations = []
        total_records = 0

        with self._get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # 1. Stores
                cur.execute("SELECT store_id, store_name, region, opened_on FROM stores;")
                stores = [self.inject_audit_metadata(dict(r)) for r in cur.fetchall()]
                if stores:
                    df_stores = pd.DataFrame(stores)
                    loc = self.writer.write_parquet(df_stores, "pos", "stores", date_str)
                    s3_locations.append(loc)
                    total_records += len(stores)

                # 2. Products
                cur.execute("SELECT product_id, product_name, category, list_price FROM products;")
                products = [self.inject_audit_metadata(dict(r)) for r in cur.fetchall()]
                if products:
                    df_products = pd.DataFrame(products)
                    loc = self.writer.write_parquet(df_products, "pos", "products", date_str)
                    s3_locations.append(loc)
                    total_records += len(products)

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        return ExtractionResult(
            source_name="pos_dimensions",
            records_extracted=total_records,
            bytes_written=0,
            low_watermark=None,
            high_watermark=None,
            s3_locations=s3_locations,
            duration_seconds=duration,
        )

    def extract(self, low_watermark: Optional[datetime] = None) -> ExtractionResult:
        """Incrementally extract pos_transactions based on watermark control table."""
        start_time = datetime.now(timezone.utc)
        date_str = start_time.strftime("%Y-%m-%d")

        if low_watermark is None:
            low_watermark, high_watermark = self.watermark_mgr.get_watermark(
                pipeline_name="retailpulse_daily_pipeline",
                source_name="pos",
                entity_name="transactions",
                lookback_hours=2,
            )
        else:
            high_watermark = start_time

        logger.info(
            f"[POSExtractor] Querying transactions with watermark range: "
            f"[{low_watermark.isoformat()} -> {high_watermark.isoformat()}]"
        )

        query = """
            SELECT
                transaction_id,
                store_id,
                product_id,
                customer_id,
                quantity,
                unit_price,
                txn_ts,
                updated_at,
                payment_method
            FROM pos_transactions
            WHERE updated_at >= %s AND updated_at < %s
            ORDER BY updated_at ASC;
        """

        s3_locations = []
        total_rows = 0
        latest_updated_at = low_watermark

        with self._get_connection() as conn:
            with conn.cursor(name="pos_txn_cursor", cursor_factory=RealDictCursor) as cur:
                cur.itersize = self.chunk_size
                cur.execute(query, (low_watermark, high_watermark))

                while True:
                    rows = cur.fetchmany(self.chunk_size)
                    if not rows:
                        break

                    chunk_records = []
                    for r in rows:
                        row_dict = dict(r)
                        if row_dict["updated_at"] and row_dict["updated_at"] > latest_updated_at:
                            latest_updated_at = row_dict["updated_at"]
                        chunk_records.append(self.inject_audit_metadata(row_dict))

                    df_chunk = pd.DataFrame(chunk_records)
                    loc = self.writer.write_parquet(
                        df_chunk,
                        source_name="pos",
                        entity_name="transactions",
                        snapshot_date=date_str,
                    )
                    s3_locations.append(loc)
                    total_rows += len(chunk_records)

        # Advance watermark on successful extraction
        if total_rows > 0:
            self.watermark_mgr.commit_watermark(
                pipeline_name="retailpulse_daily_pipeline",
                source_name="pos",
                entity_name="transactions",
                new_watermark=latest_updated_at,
                records_processed=total_rows,
                status="SUCCESS",
            )

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(f"[POSExtractor] Completed extraction: {total_rows} rows extracted in {duration:.2f}s.")
        return ExtractionResult(
            source_name="pos",
            records_extracted=total_rows,
            bytes_written=0,
            low_watermark=low_watermark,
            high_watermark=latest_updated_at,
            s3_locations=s3_locations,
            duration_seconds=duration,
        )
