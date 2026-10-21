"""Unit tests for WatermarkManager state and lookback window logic."""
import os
import tempfile
import pytest
from datetime import datetime, timezone, timedelta

from ingestion.watermark import WatermarkManager


def test_watermark_retrieval_and_lookback():
    """Verify that watermark retrieves state and properly applies the lookback window."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_watermarks.db")
        mgr = WatermarkManager(local_path=db_path)

        now = datetime.now(timezone.utc)
        mgr.commit_watermark(
            pipeline_name="test_pipeline",
            source_name="pos",
            entity_name="transactions",
            new_watermark=now,
            records_processed=100,
            status="SUCCESS",
        )

        low, high = mgr.get_watermark(
            pipeline_name="test_pipeline",
            source_name="pos",
            entity_name="transactions",
            lookback_hours=2,
        )

        # Low watermark should be approximately now - 2 hours
        assert low < now
        assert abs((now - low).total_seconds() - 7200) < 5
