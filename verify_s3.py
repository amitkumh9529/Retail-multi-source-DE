import os
import boto3
from dotenv import load_dotenv

load_dotenv()
s3 = boto3.client("s3")
bucket = os.getenv("AWS_S3_BUCKET_RAW")

print(f"============================================================")
print(f"AWS S3 Data Lake Verification: s3://{bucket}/")
print(f"============================================================")

prefixes = [
    "raw/threepl/inventory/",
    "raw/threepl/shipments/",
    "raw/pos/stores/",
    "raw/pos/products/",
    "raw/pos/transactions/",
    "raw/ecommerce/orders/",
    "raw/ecommerce/order_items/",
    "dead-letter-quarantine/",
]

total_files = 0
total_size_bytes = 0

for prefix in prefixes:
    res = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    contents = res.get("Contents", [])
    count = len(contents)
    size_kb = round(sum(o["Size"] for o in contents) / 1024, 1)
    total_files += count
    total_size_bytes += sum(o["Size"] for o in contents)
    print(f"[{count:02d} files | {size_kb:7.1f} KB]  {prefix}")

print(f"------------------------------------------------------------")
print(f"Total Objects Landed: {total_files}")
print(f"Total Data Volume   : {round(total_size_bytes / (1024 * 1024), 2)} MB")
print(f"============================================================")
