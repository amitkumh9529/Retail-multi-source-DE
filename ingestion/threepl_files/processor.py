"""
RetailPulse 3PL Partner File Ingestion Processor.

Ingests daily 3PL inventory and shipment CSV drops from landing zone (S3 or local):
- Validates SHA-256 checksums before parsing.
- Intercepts zero-byte/corrupt files and quarantines them to Dead-Letter Queue (DLQ).
- Handles schema drift (e.g. missing `updated_at` column in older extracts).
- Injects audit lineage metadata and converts to compressed Parquet format.
"""
from datetime import datetime, timezone
import hashlib
import logging
import os
from typing import Dict, List, Optional, Tuple
import pandas as pd

from ingestion.base import BaseExtractor, ExtractionResult
from ingestion.s3_uploader import DataLakeWriter

logger = logging.getLogger("retailpulse.threepl.processor")


class ThreePLFileProcessor(BaseExtractor):
    def __init__(
        self,
        landing_dir: Optional[str] = None,
        writer: Optional[DataLakeWriter] = None,
    ):
        super().__init__(source_name="threepl")
        self.landing_dir = landing_dir or os.getenv("THREEPL_LOCAL_RAW_DIR", "./data_source/raw/3pl")
        self.writer = writer or DataLakeWriter()

    def _verify_sha256(self, file_path: str) -> bool:
        """Verify file integrity against companion .sha256 file if present."""
        checksum_path = file_path + ".sha256"
        if not os.path.exists(checksum_path):
            logger.warning(f"[ThreePL] No companion .sha256 found for {file_path}. Skipping hash check.")
            return True

        h = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        calculated = h.hexdigest().strip()

        with open(checksum_path, "r") as f:
            expected = f.read().strip()

        if calculated != expected:
            logger.error(f"[ChecksumMismatch] File {file_path} failed SHA-256 check! Exp: {expected}, Got: {calculated}")
            return False
        return True

    def process_file(
        self,
        file_path: str,
        entity_name: str,
        snapshot_date: str,
    ) -> Tuple[Optional[str], Optional[str], int]:
        """
        Processes a single 3PL CSV file:
        Validates size, checksum, schema, converts to Parquet, and writes to Data Lake.
        Returns: (success_parquet_loc, dlq_loc, row_count)
        """
        # 1. Check for zero-byte or unreadable file
        file_size = os.path.getsize(file_path)
        if file_size == 0:
            dlq_loc = self.writer.write_dead_letter(
                raw_payload=f"File {file_path} is 0 bytes (corrupt extract)",
                source_name="threepl",
                entity_name=entity_name,
                error_message="ZeroByteFileCorrupt",
            )
            logger.error(f"[CorruptFile] Quarantined zero-byte file {file_path} to {dlq_loc}")
            return None, dlq_loc, 0

        # 2. Verify SHA-256 Checksum
        if not self._verify_sha256(file_path):
            dlq_loc = self.writer.write_dead_letter(
                raw_payload=f"File {file_path} failed SHA256 checksum verification",
                source_name="threepl",
                entity_name=entity_name,
                error_message="ChecksumMismatch",
            )
            return None, dlq_loc, 0

        # 3. Read CSV and handle schema drift
        try:
            df = pd.read_csv(file_path)
        except Exception as e:
            dlq_loc = self.writer.write_dead_letter(
                raw_payload=f"Failed reading CSV {file_path}: {e}",
                source_name="threepl",
                entity_name=entity_name,
                error_message=str(e),
            )
            return None, dlq_loc, 0

        # Schema drift handling: backfill missing `updated_at` column if omitted by 3PL
        if "updated_at" not in df.columns:
            logger.warning(f"[SchemaDrift] File {file_path} missing 'updated_at' column. Imputing from snapshot_date.")
            df["updated_at"] = f"{snapshot_date}T02:00:00Z"
            df["_schema_drift_detected"] = True
        else:
            df["_schema_drift_detected"] = False

        # 4. Inject audit metadata
        records = [self.inject_audit_metadata(row) for row in df.to_dict(orient="records")]
        enriched_df = pd.DataFrame(records)

        # 5. Write to partitioned Parquet
        parquet_loc = self.writer.write_parquet(
            enriched_df,
            source_name="threepl",
            entity_name=entity_name,
            snapshot_date=snapshot_date,
        )
        return parquet_loc, None, len(enriched_df)

    def extract(self, low_watermark: Optional[datetime] = None) -> ExtractionResult:
        """Scan landing directory for 3PL files and process all unprocessed files."""
        start_time = datetime.now(timezone.utc)
        total_rows = 0
        total_errors = 0
        s3_locations = []
        quarantine_locations = []

        if not os.path.exists(self.landing_dir):
            logger.info(f"[ThreePL] Landing directory {self.landing_dir} does not exist yet. No files to process.")
            return ExtractionResult("threepl", 0, 0, None, None)

        for root, _, files in os.walk(self.landing_dir):
            for file in files:
                if not file.endswith(".csv"):
                    continue

                full_path = os.path.join(root, file)

                # Determine entity and snapshot date from directory / filename
                entity_name = "inventory" if "inventory" in file or "inventory" in root else "shipments"
                snapshot_date = start_time.strftime("%Y-%m-%d")

                # Parse snapshot_date from path e.g. snapshot_date=YYYY-MM-DD
                for part in root.split(os.sep):
                    if part.startswith("snapshot_date="):
                        snapshot_date = part.split("=")[1]

                parquet_loc, dlq_loc, row_count = self.process_file(full_path, entity_name, snapshot_date)
                if parquet_loc:
                    s3_locations.append(parquet_loc)
                    total_rows += row_count
                if dlq_loc:
                    quarantine_locations.append(dlq_loc)
                    total_errors += 1

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(
            f"[ThreePL] Completed processing: {total_rows} rows extracted, "
            f"{total_errors} quarantined in {duration:.2f}s."
        )
        return ExtractionResult(
            source_name="threepl",
            records_extracted=total_rows,
            bytes_written=0,
            low_watermark=None,
            high_watermark=None,
            s3_locations=s3_locations,
            quarantine_locations=quarantine_locations,
            errors_encountered=total_errors,
            duration_seconds=duration,
        )
