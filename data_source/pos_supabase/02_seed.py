"""
Seed the Supabase PostgreSQL POS database with realistic transactions.

- Bulk-inserts stores, products, and transactions using execute_values
- Injects ~1% late-arriving updates (old rows whose updated_at is bumped to now)
- Idempotent enough to re-run by truncating first
"""
import os
import random
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from faker import Faker
from faker.providers import commerce

load_dotenv()

DSN             = os.environ["SUPABASE_DB_URL"]
N_STORES        = int(os.getenv("POS_SEED_STORES", "200"))
N_PRODUCTS      = int(os.getenv("POS_SEED_PRODUCTS", "5000"))
N_TXN           = int(os.getenv("POS_SEED_TXN", "250000"))
LATE_PCT        = float(os.getenv("POS_SEED_LATE_PCT", "0.01"))
BATCH           = 5000

random.seed(42)
Faker.seed(42)
fake = Faker()
fake.add_provider(commerce)

REGIONS   = ["northeast", "southeast", "midwest", "southwest", "west"]
CATEGORIES = ["apparel", "electronics", "grocery", "home", "sports", "beauty"]
PAYMENTS  = ["card", "cash", "mobile", "gift_card"]


def main():
    now = datetime.now(timezone.utc)
    conn = psycopg2.connect(DSN, sslmode="require")
    conn.autocommit = False
    cur = conn.cursor()

    print("Truncating tables…")
    cur.execute("truncate table pos_transactions, products, stores restart identity;")

    print(f"Seeding {N_STORES} stores…")
    stores = [
        (i, f"{fake.city()} Store #{i}", random.choice(REGIONS), fake.date_between("-10y", "-1y"))
        for i in range(1, N_STORES + 1)
    ]
    execute_values(cur, "insert into stores (store_id, store_name, region, opened_on) values %s", stores)

    print(f"Seeding {N_PRODUCTS} products…")
    products = [
        (
            f"SKU-{1000 + i}",
            fake.commerce.product_name(),
            random.choice(CATEGORIES),
            round(random.uniform(3, 400), 2),
        )
        for i in range(N_PRODUCTS)
    ]
    execute_values(cur, "insert into products (product_id, product_name, category, list_price) values %s", products)

    print(f"Seeding {N_TXN} transactions…")
    buffer = []
    for i in range(N_TXN):
        store_id   = random.randint(1, N_STORES)
        product_id = f"SKU-{1000 + random.randrange(N_PRODUCTS)}"
        qty        = random.choice([-1, 1, 1, 1, 2, 2, 3])  # occasional refunds
        price      = round(random.uniform(3, 400), 2)
        txn_ts     = now - timedelta(days=random.uniform(0, 30), seconds=random.uniform(0, 86400))
        updated_at = txn_ts + timedelta(minutes=random.uniform(0, 10))

        # 1% late-arriving corrections: old txn, but updated "recently"
        if random.random() < LATE_PCT:
            updated_at = now - timedelta(minutes=random.uniform(0, 60))

        buffer.append((
            store_id, product_id, f"CUST-{random.randint(1, 20000)}",
            qty, price, txn_ts, updated_at, random.choice(PAYMENTS),
        ))
        if len(buffer) >= BATCH:
            execute_values(
                cur,
                "insert into pos_transactions "
                "(store_id, product_id, customer_id, quantity, unit_price, txn_ts, updated_at, payment_method) "
                "values %s",
                buffer,
            )
            buffer.clear()

    if buffer:
        execute_values(
            cur,
            "insert into pos_transactions "
            "(store_id, product_id, customer_id, quantity, unit_price, txn_ts, updated_at, payment_method) "
            "values %s",
            buffer,
        )

    conn.commit()

    cur.execute("select count(*) from pos_transactions")
    print(f"Total rows: {cur.fetchone()[0]}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()