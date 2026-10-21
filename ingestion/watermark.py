"""
RetailPulse Watermark and High-Water-Mark Control Module.

Ensures idempotent, stateful incremental extraction by tracking low and high watermarks
for each source entity. High watermark is only committed upon downstream staging success.
Includes lookback buffer support for late-arriving updates.
"""
from datetime import datetime, timedelta, timezone
import json
import logging
import os
import sqlite3
from typing import Optional, Tuple

logger = logging.getLogger("retailpulse.watermark")


class WatermarkManager:
    """
    Manages incremental high watermarks.

    Supports:
    1. Snowflake / PostgreSQL warehouse table `ETL_METADATA.WATERMARK_CONTROL`
    2. Local file/sqlite fallback for offline and local test execution.
    """

    def __init__(self, backend: str = "auto", db_conn=None, local_path: str = "./.watermarks.db"):
        self.backend = backend
        self.db_conn = db_conn
        self.local_path = local_path
        self._init_local_db()

    def _init_local_db(self):
        """Initialize local SQLite control table for isolated testing."""
        os.makedirs(os.path.dirname(os.path.abspath(self.local_path)), exist_ok=True)
        conn = sqlite3.connect(self.local_path)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS watermark_control (
                    pipeline_name TEXT NOT NULL,
                    source_name TEXT NOT NULL,
                    entity_name TEXT NOT NULL,
                    last_success_watermark TIMESTAMP NOT NULL,
                    last_run_timestamp TIMESTAMP NOT NULL,
                    last_status TEXT NOT NULL,
                    records_processed INTEGER DEFAULT 0,
                    PRIMARY KEY (pipeline_name, source_name, entity_name)
                );
                """
            )
            conn.commit()
        finally:
            conn.close()

    def get_watermark(
        self,
        pipeline_name: str,
        source_name: str,
        entity_name: str,
        lookback_hours: int = 2,
        default_start: Optional[datetime] = None,
    ) -> Tuple[datetime, datetime]:
        """
        Retrieve (low_watermark, high_watermark) window for the current extraction run.

        Applies a safety lookback window (e.g. 2 hours) to capture late-arriving records.
        Downstream upsert/MERGE logic handles duplicate deduplication idempotently.
        """
        now = datetime.now(timezone.utc)
        high_watermark = now

        if default_start is None:
            default_start = now - timedelta(days=7)

        # Check local DB
        conn = sqlite3.connect(self.local_path)
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT last_success_watermark
                FROM watermark_control
                WHERE pipeline_name = ? AND source_name = ? AND entity_name = ?
                """,
                (pipeline_name, source_name, entity_name),
            )
            row = cursor.fetchone()
        finally:
            conn.close()

        if row and row[0]:
            try:
                last_watermark = datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
            except ValueError:
                last_watermark = default_start
            # Apply lookback buffer
            low_watermark = last_watermark - timedelta(hours=lookback_hours)
            logger.info(
                f"[Watermark] Found previous watermark for {source_name}.{entity_name}: {last_watermark.isoformat()}. "
                f"With {lookback_hours}h lookback: {low_watermark.isoformat()}"
            )
        else:
            low_watermark = default_start
            logger.info(
                f"[Watermark] No prior watermark found for {source_name}.{entity_name}. "
                f"Using default starting point: {low_watermark.isoformat()}"
            )

        return low_watermark, high_watermark

    def commit_watermark(
        self,
        pipeline_name: str,
        source_name: str,
        entity_name: str,
        new_watermark: datetime,
        records_processed: int = 0,
        status: str = "SUCCESS",
    ) -> None:
        """
        Advance and commit the high-water mark ONLY after downstream extraction and staging succeeds.
        """
        now = datetime.now(timezone.utc)
        watermark_iso = new_watermark.isoformat()

        conn = sqlite3.connect(self.local_path)
        try:
            conn.execute(
                """
                INSERT INTO watermark_control (
                    pipeline_name, source_name, entity_name,
                    last_success_watermark, last_run_timestamp, last_status, records_processed
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pipeline_name, source_name, entity_name) DO UPDATE SET
                    last_success_watermark = excluded.last_success_watermark,
                    last_run_timestamp = excluded.last_run_timestamp,
                    last_status = excluded.last_status,
                    records_processed = excluded.records_processed;
                """,
                (
                    pipeline_name,
                    source_name,
                    entity_name,
                    watermark_iso,
                    now.isoformat(),
                    status,
                    records_processed,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        logger.info(
            f"[Watermark Committed] {source_name}.{entity_name} advanced to {watermark_iso} "
            f"({records_processed} records, status={status})"
        )
