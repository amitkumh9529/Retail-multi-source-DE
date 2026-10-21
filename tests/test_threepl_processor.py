"""Unit and integration tests for 3PL file processor."""
import hashlib
import os
import tempfile
import pytest
import pandas as pd

from ingestion.threepl_files.processor import ThreePLFileProcessor
from ingestion.s3_uploader import DataLakeWriter


def test_zero_byte_corrupt_file_quarantine():
    """Verify that a zero-byte file is flagged, quarantined to DLQ, and not ingested."""
    with tempfile.TemporaryDirectory() as tmpdir:
        corrupt_path = os.path.join(tmpdir, "3pl_inventory_CORRUPT_20261021.csv")
        open(corrupt_path, "wb").close()

        lake_dir = os.path.join(tmpdir, "lake")
        writer = DataLakeWriter(local_lake_root=lake_dir)
        processor = ThreePLFileProcessor(landing_dir=tmpdir, writer=writer)

        parquet_loc, dlq_loc, count = processor.process_file(corrupt_path, "inventory", "2026-10-21")
        assert parquet_loc is None
        assert dlq_loc is not None
        assert count == 0
        assert os.path.exists(dlq_loc)


def test_schema_drift_missing_updated_at():
    """Verify that CSV files missing `updated_at` have the timestamp safely backfilled."""
    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = os.path.join(tmpdir, "3pl_inventory_20261021.csv")
        df = pd.DataFrame([
            {"sku": "SKU-1001", "warehouse_id": "WH-EAST-01", "on_hand_qty": 50, "reserved_qty": 5, "snapshot_date": "2026-10-21"}
        ])
        df.to_csv(csv_path, index=False)

        # Write valid sha256 companion
        h = hashlib.sha256()
        with open(csv_path, "rb") as f:
            h.update(f.read())
        with open(csv_path + ".sha256", "w") as f:
            f.write(h.hexdigest())

        lake_dir = os.path.join(tmpdir, "lake")
        writer = DataLakeWriter(local_lake_root=lake_dir)
        processor = ThreePLFileProcessor(landing_dir=tmpdir, writer=writer)

        parquet_loc, dlq_loc, count = processor.process_file(csv_path, "inventory", "2026-10-21")
        assert dlq_loc is None
        assert parquet_loc is not None
        assert count == 1
