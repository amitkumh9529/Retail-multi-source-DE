-- ==============================================================================
-- 02_watermark_control_table.sql
-- High-Watermark State Management & Idempotent Audit Trail
-- ==============================================================================

USE DATABASE RETAILPULSE_DW;
USE SCHEMA ETL_METADATA;

-- 1. Control Table for Tracking State Across Sources and Pipelines
CREATE TABLE IF NOT EXISTS WATERMARK_CONTROL (
    pipeline_name           VARCHAR(100) NOT NULL,
    source_name             VARCHAR(50)  NOT NULL,
    entity_name             VARCHAR(100) NOT NULL,
    last_success_watermark  TIMESTAMP_NTZ NOT NULL,
    last_run_timestamp      TIMESTAMP_NTZ NOT NULL,
    last_run_batch_id       VARCHAR(100),
    last_status             VARCHAR(20)  NOT NULL,
    records_processed       NUMBER(12,0) DEFAULT 0,
    created_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (pipeline_name, source_name, entity_name)
);

-- Seed initial watermarks (defaulting to 30 days ago for cold start)
MERGE INTO WATERMARK_CONTROL AS target
USING (
    SELECT 'retailpulse_daily_pipeline' AS pipeline_name, 'ecommerce' AS source_name, 'orders' AS entity_name, DATEADD(day, -30, CURRENT_TIMESTAMP()) AS last_success_watermark, CURRENT_TIMESTAMP() AS last_run_timestamp, 'INITIALIZED' AS last_status
    UNION ALL
    SELECT 'retailpulse_daily_pipeline', 'pos', 'transactions', DATEADD(day, -30, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'INITIALIZED'
    UNION ALL
    SELECT 'retailpulse_daily_pipeline', 'threepl', 'inventory', DATEADD(day, -30, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'INITIALIZED'
    UNION ALL
    SELECT 'retailpulse_daily_pipeline', 'threepl', 'shipments', DATEADD(day, -30, CURRENT_TIMESTAMP()), CURRENT_TIMESTAMP(), 'INITIALIZED'
) AS src
ON target.pipeline_name = src.pipeline_name
   AND target.source_name = src.source_name
   AND target.entity_name = src.entity_name
WHEN NOT MATCHED THEN
    INSERT (pipeline_name, source_name, entity_name, last_success_watermark, last_run_timestamp, last_status)
    VALUES (src.pipeline_name, src.source_name, src.entity_name, src.last_success_watermark, src.last_run_timestamp, src.last_status);


-- 2. Stored Procedure for Two-Phase Safe Watermark Advancement
CREATE OR REPLACE PROCEDURE SP_ADVANCE_WATERMARK(
    P_PIPELINE_NAME     VARCHAR,
    P_SOURCE_NAME       VARCHAR,
    P_ENTITY_NAME       VARCHAR,
    P_NEW_WATERMARK     VARCHAR,
    P_BATCH_ID          VARCHAR,
    P_RECORDS_PROCESSED FLOAT,
    P_STATUS            VARCHAR
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    MERGE INTO WATERMARK_CONTROL AS target
    USING (
        SELECT
            :P_PIPELINE_NAME AS pipeline_name,
            :P_SOURCE_NAME   AS source_name,
            :P_ENTITY_NAME   AS entity_name,
            TO_TIMESTAMP_NTZ(:P_NEW_WATERMARK) AS new_watermark,
            CURRENT_TIMESTAMP() AS run_ts,
            :P_BATCH_ID AS batch_id,
            :P_RECORDS_PROCESSED AS rec_count,
            :P_STATUS AS status
    ) AS src
    ON target.pipeline_name = src.pipeline_name
       AND target.source_name = src.source_name
       AND target.entity_name = src.entity_name
    WHEN MATCHED AND src.status = 'SUCCESS' THEN
        UPDATE SET
            target.last_success_watermark = src.new_watermark,
            target.last_run_timestamp     = src.run_ts,
            target.last_run_batch_id      = src.batch_id,
            target.last_status            = src.status,
            target.records_processed      = src.rec_count,
            target.updated_at             = CURRENT_TIMESTAMP()
    WHEN MATCHED AND src.status != 'SUCCESS' THEN
        UPDATE SET
            target.last_run_timestamp     = src.run_ts,
            target.last_run_batch_id      = src.batch_id,
            target.last_status            = src.status,
            target.updated_at             = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (pipeline_name, source_name, entity_name, last_success_watermark, last_run_timestamp, last_run_batch_id, last_status, records_processed)
        VALUES (src.pipeline_name, src.source_name, src.entity_name, src.new_watermark, src.run_ts, src.batch_id, src.status, src.rec_count);

    RETURN 'Watermark advanced successfully for ' || :P_SOURCE_NAME || '.' || :P_ENTITY_NAME;
END;
$$;
