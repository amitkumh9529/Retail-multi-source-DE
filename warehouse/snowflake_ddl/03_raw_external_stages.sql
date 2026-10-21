-- ==============================================================================
-- 03_raw_external_stages.sql
-- S3 Storage Integration, External Stages, and Raw Ingestion Tables
-- ==============================================================================

USE DATABASE RETAILPULSE_DW;
USE SCHEMA RAW;

-- 1. File Formats
CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY';

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;

-- 2. External S3 Stage (Requires AWS IAM Role with S3 Read Policy)
-- Placeholder for production S3 integration
CREATE OR REPLACE STAGE S3_RAW_STAGE
    URL = 's3://retailpulse-raw-data-prod/'
    -- STORAGE_INTEGRATION = retailpulse_s3_int
    FILE_FORMAT = PARQUET_FORMAT;

-- 3. Raw Tables with Audit Lineage Columns
-- Raw tables are immutable and append-only. Reprocessing simply copies new files.

CREATE TABLE IF NOT EXISTS RAW.ECOMMERCE_ORDERS_RAW (
    order_id        VARCHAR(64),
    customer_id     VARCHAR(64),
    order_ts        TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    status          VARCHAR(32),
    currency        VARCHAR(8),
    payment_method  VARCHAR(32),
    total_amount    NUMBER(12,2),
    _extracted_at   TIMESTAMP_NTZ,
    _source_system  VARCHAR(32),
    _batch_id       VARCHAR(128),
    _record_hash    VARCHAR(64),
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.ECOMMERCE_ORDER_ITEMS_RAW (
    order_id        VARCHAR(64),
    line_item_id    NUMBER(8,0),
    product_id      VARCHAR(64),
    quantity        NUMBER(8,0),
    unit_price      NUMBER(12,2),
    order_ts        TIMESTAMP_NTZ,
    _extracted_at   TIMESTAMP_NTZ,
    _source_system  VARCHAR(32),
    _batch_id       VARCHAR(128),
    _record_hash    VARCHAR(64),
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.POS_TRANSACTIONS_RAW (
    transaction_id  NUMBER(19,0),
    store_id        NUMBER(8,0),
    product_id      VARCHAR(64),
    customer_id     VARCHAR(64),
    quantity        NUMBER(8,0),
    unit_price      NUMBER(12,2),
    txn_ts          TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    payment_method  VARCHAR(32),
    _extracted_at   TIMESTAMP_NTZ,
    _source_system  VARCHAR(32),
    _batch_id       VARCHAR(128),
    _record_hash    VARCHAR(64),
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.POS_STORES_RAW (
    store_id        NUMBER(8,0),
    store_name      VARCHAR(128),
    region          VARCHAR(64),
    opened_on       DATE,
    _extracted_at   TIMESTAMP_NTZ,
    _source_system  VARCHAR(32),
    _batch_id       VARCHAR(128),
    _record_hash    VARCHAR(64),
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.POS_PRODUCTS_RAW (
    product_id      VARCHAR(64),
    product_name    VARCHAR(256),
    category        VARCHAR(64),
    list_price      NUMBER(12,2),
    _extracted_at   TIMESTAMP_NTZ,
    _source_system  VARCHAR(32),
    _batch_id       VARCHAR(128),
    _record_hash    VARCHAR(64),
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.THREEPL_INVENTORY_RAW (
    sku             VARCHAR(64),
    warehouse_id    VARCHAR(64),
    on_hand_qty     NUMBER(10,0),
    reserved_qty    NUMBER(10,0),
    snapshot_date   DATE,
    updated_at      TIMESTAMP_NTZ,
    _extracted_at   TIMESTAMP_NTZ,
    _source_system  VARCHAR(32),
    _batch_id       VARCHAR(128),
    _record_hash    VARCHAR(64),
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.THREEPL_SHIPMENTS_RAW (
    shipment_id     VARCHAR(64),
    sku             VARCHAR(64),
    warehouse_id    VARCHAR(64),
    carrier         VARCHAR(32),
    status          VARCHAR(32),
    shipped_at      TIMESTAMP_NTZ,
    updated_at      TIMESTAMP_NTZ,
    _extracted_at   TIMESTAMP_NTZ,
    _source_system  VARCHAR(32),
    _batch_id       VARCHAR(128),
    _record_hash    VARCHAR(64),
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 4. Sample Copy Command with Idempotent File Tracking
-- Uses PATTERN matching and files are only loaded once per file metadata registry
-- COPY INTO RAW.POS_TRANSACTIONS_RAW
-- FROM @S3_RAW_STAGE/raw/pos/transactions/
-- FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
-- MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
-- ON_ERROR = 'SKIP_FILE';
