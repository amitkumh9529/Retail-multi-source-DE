-- ==============================================================================
-- 04_dead_letter_tables.sql
-- Ingestion Dead-Letter Queue (DLQ) & Quarantine Tables for Incident Response
-- ==============================================================================

USE DATABASE RETAILPULSE_DW;
USE SCHEMA ETL_METADATA;

CREATE TABLE IF NOT EXISTS DLQ_INGESTION_QUARANTINE (
    quarantine_id       VARCHAR(64) DEFAULT UUID_STRING(),
    source_name         VARCHAR(64) NOT NULL,
    entity_name         VARCHAR(64) NOT NULL,
    error_reason        VARCHAR(256) NOT NULL,
    raw_payload_snippet VARCHAR(16777216),
    s3_dlq_path         VARCHAR(1024),
    quarantined_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    resolved_flag       BOOLEAN DEFAULT FALSE,
    resolution_notes    VARCHAR(1024)
);

CREATE OR REPLACE VIEW V_ACTIVE_PIPELINE_INCIDENTS AS
SELECT
    source_name,
    entity_name,
    error_reason,
    COUNT(*) AS incident_count,
    MIN(quarantined_at) AS first_seen,
    MAX(quarantined_at) AS last_seen
FROM DLQ_INGESTION_QUARANTINE
WHERE resolved_flag = FALSE
GROUP BY 1, 2, 3
ORDER BY last_seen DESC;
