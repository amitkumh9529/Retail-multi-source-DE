"""
RetailPulse Production ELT Ingestion Base Module.

Provides standard abstractions, structured logging, audit metadata injection,
retry decorators with exponential backoff and jitter, and quarantine routing.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("retailpulse.ingestion")


@dataclass
class ExtractionResult:
    source_name: str
    records_extracted: int
    bytes_written: int
    low_watermark: Optional[datetime]
    high_watermark: Optional[datetime]
    s3_locations: List[str] = field(default_factory=list)
    quarantine_locations: List[str] = field(default_factory=list)
    errors_encountered: int = 0
    duration_seconds: float = 0.0


def compute_record_hash(record: Dict[str, Any], excluded_keys: Optional[List[str]] = None) -> str:
    """Compute deterministic SHA-256 hash of a dictionary for idempotency and change detection."""
    clean_dict = {
        k: v for k, v in record.items()
        if not (excluded_keys and k in excluded_keys) and not k.startswith("_")
    }
    encoded = json.dumps(clean_dict, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def with_retry(
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    exceptions: Tuple[Exception, ...] = (Exception,),
) -> Callable:
    """Production-grade retry decorator with exponential backoff and jitter."""
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        logger.error(f"[RetryExhausted] {func.__name__} failed after {max_retries} attempts: {e}")
                        raise
                    sleep_time = delay * (1 + 0.1 * (time.time() % 1))  # slight jitter
                    logger.warning(
                        f"[Attempt {attempt}/{max_retries}] {func.__name__} failed with {type(e).__name__}: {e}. "
                        f"Retrying in {sleep_time:.2f}s..."
                    )
                    time.sleep(sleep_time)
                    delay *= backoff_factor
            raise last_exception
        return wrapper
    return decorator


class BaseExtractor(ABC):
    """Abstract base class for all source system extractors."""

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.batch_id = f"batch_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{os.urandom(4).hex()}"
        self.logger = logging.getLogger(f"retailpulse.extractor.{source_name}")

    def inject_audit_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Inject lineage and audit metadata into raw records."""
        extracted_at = datetime.now(timezone.utc).isoformat()
        record_hash = compute_record_hash(record)
        return {
            **record,
            "_extracted_at": extracted_at,
            "_source_system": self.source_name,
            "_batch_id": self.batch_id,
            "_record_hash": record_hash,
        }

    @abstractmethod
    def extract(self, low_watermark: Optional[datetime] = None) -> ExtractionResult:
        """Extract data incrementally from the source."""
        pass
