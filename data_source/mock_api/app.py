"""
RetailPulse mock e-commerce Orders API.

Features that make this production-like:
- Bearer-token auth
- Limit/offset pagination with total counts
- Per-token rate limiting (HTTP 429)
- Incremental filter via `updated_since`
- Schema drift: randomly returns `total_amount_cents` instead of `total_amount`
- Broken JSON: randomly returns truncated JSON (simulates upstream incidents)
- Late-arriving updates: a background loop bumps random orders' `updated_at`
"""
import os
import time
import json
import random
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

load_dotenv()

API_TOKEN            = os.getenv("MOCK_API_TOKEN", "retailpulse-dev-token")
RATE_LIMIT_PER_MIN   = int(os.getenv("MOCK_API_RATE_LIMIT", "60"))
DRIFT_PROB           = float(os.getenv("MOCK_API_DRIFT_PROB", "0.05"))
MALFORMED_PROB       = float(os.getenv("MOCK_API_MALFORMED_PROB", "0.01"))
TOTAL_ORDERS         = int(os.getenv("MOCK_API_ORDERS", "50000"))
LATE_INTERVAL        = int(os.getenv("MOCK_API_LATE_UPDATE_INTERVAL", "60"))
LATE_BATCH           = int(os.getenv("MOCK_API_LATE_UPDATE_BATCH", "25"))

random.seed(42)
security = HTTPBearer()
app = FastAPI(title="RetailPulse Mock Orders API", version="1.0.0")


# ---------------------------------------------------------------------------
# In-memory dataset
# ---------------------------------------------------------------------------
STATUSES = ["created", "paid", "shipped", "delivered", "cancelled"]
PAYMENTS = ["card", "paypal", "applepay", "bank_transfer"]


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _make_order(idx: int, now: datetime) -> dict:
    order_ts = now - timedelta(days=random.uniform(0, 7), seconds=random.uniform(0, 86400))
    updated_at = order_ts + timedelta(minutes=random.uniform(0, 30))
    n_items = random.randint(1, 4)
    items = []
    total = 0.0
    for j in range(n_items):
        qty = random.randint(1, 3)
        price = round(random.uniform(5, 250), 2)
        total += qty * price
        items.append({
            "line_item_id": j + 1,
            "product_id": f"SKU-{random.randint(1000, 9999)}",
            "quantity": qty,
            "unit_price": price,
        })
    return {
        "order_id": f"ORD-{100000 + idx}",
        "customer_id": f"CUST-{random.randint(1, 20000)}",
        "order_ts": _iso(order_ts),
        "updated_at": _iso(updated_at),
        "status": random.choice(STATUSES),
        "currency": "USD",
        "payment_method": random.choice(PAYMENTS),
        "items": items,
        "total_amount": round(total, 2),
    }


_now0 = datetime.now(timezone.utc)
ORDERS: list[dict] = [_make_order(i, _now0) for i in range(TOTAL_ORDERS)]
ORDERS_BY_ID = {o["order_id"]: o for o in ORDERS}


# ---------------------------------------------------------------------------
# Background: simulate late-arriving corrections
# ---------------------------------------------------------------------------
def _late_update_loop():
    while True:
        time.sleep(LATE_INTERVAL)
        now = datetime.now(timezone.utc)
        picks = random.sample(ORDERS, k=min(LATE_BATCH, len(ORDERS)))
        for o in picks:
            o["updated_at"] = _iso(now)
            o["status"] = random.choice(STATUSES)
            o["total_amount"] = round(o["total_amount"] + random.uniform(-2, 2), 2)


threading.Thread(target=_late_update_loop, daemon=True).start()


# ---------------------------------------------------------------------------
# Auth + rate limiting
# ---------------------------------------------------------------------------
_buckets: dict[str, deque] = {}


def _check_rate_limit(key: str):
    now = time.time()
    bucket = _buckets.setdefault(key, deque())
    while bucket and now - bucket[0] > 60:
        bucket.popleft()
    if len(bucket) >= RATE_LIMIT_PER_MIN:
        raise HTTPException(status_code=429, detail="rate limit exceeded")
    bucket.append(now)


def auth(creds: HTTPAuthorizationCredentials = Depends(security), request: Request = None):
    if creds.credentials != API_TOKEN:
        raise HTTPException(status_code=401, detail="invalid token")
    _check_rate_limit(creds.credentials)
    return creds.credentials


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _apply_drift(payload: dict) -> dict:
    """Randomly convert `total_amount` (float) → `total_amount_cents` (int)."""
    if random.random() < DRIFT_PROB:
        for o in payload["data"]:
            cents = int(round(o.pop("total_amount") * 100))
            o["total_amount_cents"] = cents
    return payload


def _maybe_malformed() -> Optional[Response]:
    if random.random() < MALFORMED_PROB:
        return Response(
            content=b'{"data": [{"order_id": "ORD-999',   # truncated on purpose
            status_code=200,
            media_type="application/json",
        )
    return None


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok", "orders": len(ORDERS)}


@app.get("/api/v1/orders")
def list_orders(
    limit: int = Query(100, ge=1, le=100),
    offset: int = Query(0, ge=0),
    updated_since: Optional[str] = Query(None, description="ISO8601 UTC timestamp"),
    _: str = Depends(auth),
):
    malformed = _maybe_malformed()
    if malformed:
        return malformed

    rows = ORDERS
    if updated_since:
        try:
            cutoff = datetime.fromisoformat(updated_since.replace("Z", "+00:00"))
        except ValueError:
            raise HTTPException(status_code=400, detail="bad updated_since")
        rows = [o for o in rows if datetime.fromisoformat(o["updated_at"].replace("Z", "+00:00")) > cutoff]

    rows_sorted = sorted(rows, key=lambda o: o["updated_at"])
    page = rows_sorted[offset: offset + limit]
    payload = {
        "total": len(rows_sorted),
        "limit": limit,
        "offset": offset,
        "data": page,
    }
    return JSONResponse(_apply_drift(payload))


@app.get("/api/v1/orders/{order_id}")
def get_order(order_id: str, _: str = Depends(auth)):
    o = ORDERS_BY_ID.get(order_id)
    if not o:
        raise HTTPException(status_code=404, detail="not found")
    return o


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8080, reload=False)