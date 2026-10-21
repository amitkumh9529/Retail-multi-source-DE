"""
RetailPulse E-Commerce Orders Extractor.

Performs incremental extraction from E-commerce REST API, flattens orders & order items,
appends audit lineage fields, routes corrupted responses to Dead-Letter Queue (DLQ),
and commits high-water mark upon successful upload.
"""
from datetime import datetime, timezone
import logging
from typing import Dict, List, Optional
import pandas as pd

from ingestion.base import BaseExtractor, ExtractionResult
from ingestion.ecommerce_api.client import EcommerceApiClient
from ingestion.s3_uploader import DataLakeWriter
from ingestion.watermark import WatermarkManager

logger = logging.getLogger("retailpulse.ecommerce.extractor")


class EcommerceOrdersExtractor(BaseExtractor):
    def __init__(
        self,
        api_client: Optional[EcommerceApiClient] = None,
        writer: Optional[DataLakeWriter] = None,
        watermark_mgr: Optional[WatermarkManager] = None,
    ):
        super().__init__(source_name="ecommerce")
        self.client = api_client or EcommerceApiClient()
        self.writer = writer or DataLakeWriter()
        self.watermark_mgr = watermark_mgr or WatermarkManager()

    def extract(
        self,
        low_watermark: Optional[datetime] = None,
        limit_per_page: int = 100,
        max_pages: Optional[int] = None,
    ) -> ExtractionResult:
        start_time = datetime.now(timezone.utc)

        if low_watermark is None:
            low_watermark, high_watermark = self.watermark_mgr.get_watermark(
                pipeline_name="retailpulse_daily_pipeline",
                source_name="ecommerce",
                entity_name="orders",
                lookback_hours=2,
            )
        else:
            high_watermark = start_time

        logger.info(
            f"[EcommerceExtractor] Starting extraction with watermark range: "
            f"[{low_watermark.isoformat()} -> {high_watermark.isoformat()}]"
        )

        offset = 0
        total_extracted = 0
        total_errors = 0
        s3_locations = []
        quarantine_locations = []
        page_num = 0

        raw_orders_list: List[Dict] = []
        raw_items_list: List[Dict] = []
        latest_updated_seen = low_watermark

        while True:
            page_num += 1
            if max_pages and page_num > max_pages:
                logger.info(f"[EcommerceExtractor] Reached max test pages limit ({max_pages}). Stopping.")
                break

            payload, raw_err, err_msg = self.client.fetch_orders_page(
                limit=limit_per_page,
                offset=offset,
                updated_since=low_watermark,
            )

            if raw_err:
                # Malformed JSON or upstream failure - quarantine to DLQ without terminating pipeline
                total_errors += 1
                dlq_loc = self.writer.write_dead_letter(
                    raw_payload=raw_err,
                    source_name="ecommerce",
                    entity_name="orders",
                    error_message=err_msg or "Unknown parse error",
                )
                quarantine_locations.append(dlq_loc)
                # Advance offset to avoid infinite loop on bad page
                offset += limit_per_page
                continue

            if not payload or not payload.get("data"):
                logger.info(f"[EcommerceExtractor] No more records found. Total orders processed: {total_extracted}")
                break

            orders = payload["data"]
            total_extracted += len(orders)

            for order in orders:
                items = order.pop("items", [])
                order_with_audit = self.inject_audit_metadata(order)
                raw_orders_list.append(order_with_audit)

                # Track latest updated_at for high watermark
                if "updated_at" in order:
                    try:
                        order_dt = datetime.fromisoformat(order["updated_at"].replace("Z", "+00:00"))
                        if order_dt > latest_updated_seen:
                            latest_updated_seen = order_dt
                    except ValueError:
                        pass

                # Flatten Line Items
                for item in items:
                    item_record = {
                        "order_id": order["order_id"],
                        "line_item_id": item.get("line_item_id"),
                        "product_id": item.get("product_id"),
                        "quantity": item.get("quantity"),
                        "unit_price": item.get("unit_price"),
                        "order_ts": order.get("order_ts"),
                    }
                    raw_items_list.append(self.inject_audit_metadata(item_record))

            offset += limit_per_page
            if offset >= payload.get("total", 0):
                break

        # Write batch outputs to Parquet in raw data lake
        if raw_orders_list:
            orders_df = pd.DataFrame(raw_orders_list)
            orders_loc = self.writer.write_parquet(
                orders_df,
                source_name="ecommerce",
                entity_name="orders",
                snapshot_date=start_time.strftime("%Y-%m-%d"),
            )
            s3_locations.append(orders_loc)

            items_df = pd.DataFrame(raw_items_list)
            items_loc = self.writer.write_parquet(
                items_df,
                source_name="ecommerce",
                entity_name="order_items",
                snapshot_date=start_time.strftime("%Y-%m-%d"),
            )
            s3_locations.append(items_loc)

            # Advance and commit watermark
            self.watermark_mgr.commit_watermark(
                pipeline_name="retailpulse_daily_pipeline",
                source_name="ecommerce",
                entity_name="orders",
                new_watermark=latest_updated_seen,
                records_processed=total_extracted,
                status="SUCCESS",
            )

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        return ExtractionResult(
            source_name="ecommerce",
            records_extracted=total_extracted,
            bytes_written=0,
            low_watermark=low_watermark,
            high_watermark=latest_updated_seen,
            s3_locations=s3_locations,
            quarantine_locations=quarantine_locations,
            errors_encountered=total_errors,
            duration_seconds=duration,
        )
