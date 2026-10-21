"""
RetailPulse E-Commerce REST API Client.

Handles:
- Bearer-token authentication
- HTTP 429 rate limiting with adaptive exponential backoff
- Schema drift normalization (total_amount vs total_amount_cents)
- Malformed JSON detection and quarantine isolation
"""
from datetime import datetime
import json
import logging
import os
import time
from typing import Any, Dict, Generator, List, Optional, Tuple
import requests

logger = logging.getLogger("retailpulse.ecommerce.client")


class EcommerceApiClient:
    def __init__(
        self,
        base_url: Optional[str] = None,
        token: Optional[str] = None,
        rate_limit_rps: float = 1.0,
        max_retries: int = 5,
    ):
        self.base_url = (base_url or os.getenv("ECOMM_API_BASE_URL", "http://localhost:8080")).rstrip("/")
        self.token = token or os.getenv("ECOMM_API_TOKEN", "retailpulse-dev-token")
        self.rate_limit_delay = 1.0 / max(rate_limit_rps, 0.1)
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.token}",
            "User-Agent": "RetailPulse-ELT-Pipeline/1.0",
            "Accept": "application/json",
        })

    def fetch_orders_page(
        self,
        limit: int = 100,
        offset: int = 0,
        updated_since: Optional[datetime] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[str]]:
        """
        Fetch a single page of orders with rate limit and corruption handling.

        Returns: (parsed_data, raw_error_payload, error_message)
        """
        params: Dict[str, Any] = {"limit": limit, "offset": offset}
        if updated_since:
            # Ensure UTC ISO format with Z
            params["updated_since"] = updated_since.strftime("%Y-%m-%dT%H:%M:%SZ")

        url = f"{self.base_url}/api/v1/orders"
        delay = 1.0

        for attempt in range(1, self.max_retries + 1):
            time.sleep(self.rate_limit_delay)
            try:
                response = self.session.get(url, params=params, timeout=15)

                # Handle 429 Too Many Requests
                if response.status_code == 429:
                    sleep_time = delay * (1.5 ** attempt)
                    logger.warning(f"[RateLimit 429] Received 429 from orders API. Throttling for {sleep_time:.2f}s...")
                    time.sleep(sleep_time)
                    continue

                if response.status_code == 401:
                    raise PermissionError("E-commerce API Bearer token invalid or expired.")

                response.raise_for_status()

                # Handle broken / truncated JSON responses (simulated upstream incidents)
                try:
                    payload = response.json()
                    normalized_payload = self._normalize_schema_drift(payload)
                    return normalized_payload, None, None
                except json.JSONDecodeError as decode_err:
                    logger.error(f"[MalformedJSON] Received truncated JSON response: {decode_err}")
                    return None, response.text, f"Truncated or unparseable JSON: {decode_err}"

            except requests.RequestException as req_err:
                if attempt == self.max_retries:
                    logger.error(f"[NetworkError] Failed after {self.max_retries} attempts: {req_err}")
                    raise
                time.sleep(delay)
                delay *= 2

        return None, None, "Max retries exceeded"

    def _normalize_schema_drift(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Standardize schema drift variations:
        When API drifts and emits `total_amount_cents` instead of `total_amount`,
        convert cents to dollars float while logging the drift event.
        """
        orders = payload.get("data", [])
        for order in orders:
            if "total_amount_cents" in order and "total_amount" not in order:
                order["total_amount"] = round(order.pop("total_amount_cents") / 100.0, 2)
                order["_schema_drift_detected"] = True
            else:
                order["_schema_drift_detected"] = False
        return payload
