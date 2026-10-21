"""Unit and integration tests for E-commerce REST API client & extractor."""
import json
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from ingestion.ecommerce_api.client import EcommerceApiClient
from ingestion.ecommerce_api.extractor import EcommerceOrdersExtractor


def test_schema_drift_normalization():
    """Verify that `total_amount_cents` is correctly converted to `total_amount` in dollars."""
    client = EcommerceApiClient(base_url="http://mock-test", token="test-token")
    drifted_payload = {
        "data": [
            {
                "order_id": "ORD-123",
                "total_amount_cents": 12550,
            }
        ]
    }
    normalized = client._normalize_schema_drift(drifted_payload)
    order = normalized["data"][0]
    assert "total_amount" in order
    assert order["total_amount"] == 125.50
    assert order["_schema_drift_detected"] is True


def test_malformed_json_quarantine():
    """Verify that malformed/truncated JSON responses are caught and routed to error handler."""
    client = EcommerceApiClient(base_url="http://mock-test", token="test-token")

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.side_effect = json.JSONDecodeError("Unterminated string", "doc", 20)
    mock_resp.text = '{"data": [{"order_id": "ORD-999'

    with patch.object(client.session, "get", return_value=mock_resp):
        payload, raw_err, err_msg = client.fetch_orders_page()
        assert payload is None
        assert 'ORD-999' in raw_err
        assert "Truncated" in err_msg
