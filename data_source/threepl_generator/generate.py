"""
Generate synthetic 3PL inventory + shipment CSVs.

Output per day:
  raw/3pl/inventory/snapshot_date=YYYY-MM-DD/3pl_inventory_YYYYMMDD.csv
  raw/3pl/inventory/snapshot_date=YYYY-MM-DD/3pl_inventory_YYYYMMDD.csv.sha256
  raw/3pl/shipments/snapshot_date=YYYY-MM-DD/3pl_shipments_YYYYMMDD.csv
  raw/3pl/shipments/snapshot_date=YYYY-MM-DD/3pl_shipments_YYYYMMDD.csv.sha256

Deliberate anomalies (fraction controlled by THREEPL_ANOMALY_RATE):
  - negative on_hand_qty
  - missing sku
  - duplicate rows
  - unknown warehouse_id
  - once per 7 days: a file missing the `updated_at` column (schema drift)
  - one zero-byte corrupt file (tests dead-letter handling)
"""
import csv
import hashlib
import os
import random
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from faker import Faker

load_dotenv()

OUT_DIR      = os.getenv("THREEPL_OUT_DIR", "./raw/3pl")
DAYS         = int(os.getenv("THREEPL_DAYS", "7"))
SEED         = int(os.getenv("THREEPL_SEED", "42"))
ANOMALY_RATE = float(os.getenv("THREEPL_ANOMALY_RATE", "0.01"))

INVENTORY_FIELDS = ["sku", "warehouse_id", "on_hand_qty", "reserved_qty", "snapshot_date", "updated_at"]
SHIPMENT_FIELDS  = ["shipment_id", "sku", "warehouse_id", "carrier", "status", "shipped_at", "updated_at"]

WAREHOUSES = [f"WH-{r}-{i:02d}" for r in ["EAST", "WEST", "CENTRAL"] for i in range(1, 6)]
CARRIERS   = ["UPS", "FedEx", "USPS", "DHL"]
STATUSES   = ["label_created", "in_transit", "delivered", "returned"]

random.seed(SEED)
Faker.seed(SEED)
fake = Faker()

SKUS = [f"SKU-{1000 + i}" for i in range(5000)]


def _write_checksum(path: str) -> None:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    with open(path + ".sha256", "w") as f:
        f.write(h.hexdigest() + "\n")


def _anomalize(row: dict, fields: list[str]) -> dict:
    r = random.random()
    if r < ANOMALY_RATE * 0.5:
        row["on_hand_qty" if "on_hand_qty" in row else "quantity"] = -abs(int(row.get("on_hand_qty", 1)))
    elif r < ANOMALY_RATE * 0.8:
        row["sku"] = ""
    elif r < ANOMALY_RATE:
        row["warehouse_id"] = "WH-UNKNOWN"
    return row


def generate_inventory(day: datetime, apply_drift: bool) -> str:
    folder = os.path.join(OUT_DIR, "inventory", f"snapshot_date={day.date()}")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"3pl_inventory_{day.strftime('%Y%m%d')}.csv")

    fields = INVENTORY_FIELDS if not apply_drift else [f for f in INVENTORY_FIELDS if f != "updated_at"]

    n_rows = 100_000
    seen = set()
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for _ in range(n_rows):
            row = {
                "sku": random.choice(SKUS),
                "warehouse_id": random.choice(WAREHOUSES),
                "on_hand_qty": random.randint(0, 5000),
                "reserved_qty": random.randint(0, 500),
                "snapshot_date": day.strftime("%Y-%m-%d"),
                "updated_at": (day + timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            row = _anomalize(row, fields)
            key = (row["sku"], row["warehouse_id"], row["snapshot_date"])
            if key in seen and random.random() < 0.5:
                continue   # occasional dropped duplicate
            seen.add(key)
            writer.writerow({k: row[k] for k in fields})

    _write_checksum(path)
    return path


def generate_shipments(day: datetime) -> str:
    folder = os.path.join(OUT_DIR, "shipments", f"snapshot_date={day.date()}")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"3pl_shipments_{day.strftime('%Y%m%d')}.csv")

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SHIPMENT_FIELDS)
        writer.writeheader()
        for i in range(50_000):
            row = {
                "shipment_id": f"SHP-{day.strftime('%Y%m%d')}-{i:07d}",
                "sku": random.choice(SKUS),
                "warehouse_id": random.choice(WAREHOUSES),
                "carrier": random.choice(CARRIERS),
                "status": random.choice(STATUSES),
                "shipped_at": (day + timedelta(hours=random.uniform(4, 20))).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "updated_at": (day + timedelta(hours=random.uniform(4, 22))).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
            writer.writerow(row)

    _write_checksum(path)
    return path


def generate_corrupt_file(day: datetime) -> str:
    folder = os.path.join(OUT_DIR, "inventory", f"snapshot_date={day.date()}")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, f"3pl_inventory_CORRUPT_{day.strftime('%Y%m%d')}.csv")
    open(path, "wb").close()   # zero-byte file
    _write_checksum(path)
    return path


def main():
    now = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    for i in range(DAYS):
        day = now - timedelta(days=i)
        drift = (i == DAYS - 1)              # oldest day in range ships without updated_at
        inv_path = generate_inventory(day, apply_drift=drift)
        shp_path = generate_shipments(day)
        print(f"[{day.date()}] {inv_path}")
        print(f"[{day.date()}] {shp_path}")
        if i == 0:
            corrupt = generate_corrupt_file(day)
            print(f"[{day.date()}] CORRUPT {corrupt}")


if __name__ == "__main__":
    main()