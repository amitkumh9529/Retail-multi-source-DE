"""
RetailPulse S3 & Data Lake Uploader.

Uploads extracted datasets to AWS S3 (or local data lake fallback) in immutable,
append-only Hive-style partitioned Parquet or JSONL files.
Includes dead-letter quarantine (DLQ) routing for corrupt or unparseable payloads.
"""
from datetime import datetime, timezone
import json
import logging
import os
import uuid
from typing import Any, Dict, List, Optional
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger("retailpulse.s3_uploader")


class DataLakeWriter:
    """Writes immutable raw datasets to S3 with local filesystem fallback."""

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        local_lake_root: str = "./data_lake_storage",
    ):
        self.bucket_name = bucket_name or os.getenv("AWS_S3_BUCKET_RAW", "retailpulse-raw-data-dev")
        self.local_lake_root = local_lake_root
        self.s3_client = None
        self._init_client()

    def _init_client(self):
        aws_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
        if aws_key and aws_secret and aws_key != "your_aws_access_key_id":
            try:
                import boto3
                self.s3_client = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
                logger.info(f"[S3Writer] Initialized AWS S3 client targeting bucket: {self.bucket_name}")
            except Exception as e:
                logger.warning(f"[S3Writer] Could not initialize AWS S3 client ({e}). Falling back to local data lake.")
        else:
            logger.info(f"[S3Writer] No AWS credentials configured. Operating in local data lake mode ({self.local_lake_root}).")

    def write_parquet(
        self,
        df: pd.DataFrame,
        source_name: str,
        entity_name: str,
        snapshot_date: Optional[str] = None,
    ) -> str:
        """
        Write DataFrame to partitioned Parquet file.
        Path format: raw/{source_name}/{entity_name}/snapshot_date={YYYY-MM-DD}/{batch_id}.parquet
        """
        if snapshot_date is None:
            snapshot_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        batch_uuid = uuid.uuid4().hex[:12]
        file_name = f"{source_name}_{entity_name}_{batch_uuid}.parquet"
        partition_key = f"raw/{source_name}/{entity_name}/snapshot_date={snapshot_date}/{file_name}"

        # Write to local temp or local data lake
        local_dest_dir = os.path.join(self.local_lake_root, "raw", source_name, entity_name, f"snapshot_date={snapshot_date}")
        os.makedirs(local_dest_dir, exist_ok=True)
        local_file_path = os.path.join(local_dest_dir, file_name)

        table = pa.Table.from_pandas(df)
        pq.write_table(table, local_file_path, compression="snappy")

        if self.s3_client:
            try:
                self.s3_client.upload_file(local_file_path, self.bucket_name, partition_key)
                s3_uri = f"s3://{self.bucket_name}/{partition_key}"
                logger.info(f"[S3Writer] Successfully uploaded {len(df)} rows to {s3_uri}")
                return s3_uri
            except Exception as e:
                logger.error(f"[S3Writer] Failed to upload to S3: {e}. Preserved locally at {local_file_path}")

        return os.path.abspath(local_file_path)

    def write_dead_letter(
        self,
        raw_payload: Any,
        source_name: str,
        entity_name: str,
        error_message: str,
    ) -> str:
        """
        Quarantine corrupt, malformed, or unparseable records to Dead-Letter Queue (DLQ).
        Path format: dead-letter-quarantine/{source_name}/{entity_name}/date={YYYY-MM-DD}/dlq_{uuid}.json
        """
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        record_id = uuid.uuid4().hex[:12]
        dlq_filename = f"dlq_{record_id}.json"
        s3_key = f"dead-letter-quarantine/{source_name}/{entity_name}/date={date_str}/{dlq_filename}"

        dlq_payload = {
            "quarantined_at": datetime.now(timezone.utc).isoformat(),
            "source_name": source_name,
            "entity_name": entity_name,
            "error_reason": error_message,
            "raw_payload": str(raw_payload)[:10000],
        }

        local_dir = os.path.join(
            self.local_lake_root, "dead-letter-quarantine", source_name, entity_name, f"date={date_str}"
        )
        os.makedirs(local_dir, exist_ok=True)
        local_path = os.path.join(local_dir, dlq_filename)
        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(dlq_payload, f, indent=2)

        if self.s3_client:
            try:
                self.s3_client.upload_file(local_path, self.bucket_name, s3_key)
                logger.warning(f"[DLQ] Quarantined invalid record to s3://{self.bucket_name}/{s3_key}")
                return f"s3://{self.bucket_name}/{s3_key}"
            except Exception as e:
                logger.error(f"[DLQ] Failed to upload DLQ payload to S3: {e}")

        logger.warning(f"[DLQ] Quarantined invalid record locally at {local_path}")
        return local_path
