-- ============================================================
-- Government Contract Spending Anomaly Detection
-- Redshift Schema
-- ============================================================

-- Create schema
CREATE SCHEMA IF NOT EXISTS govt_spending;

-- ============================================================
-- Main contract awards table
-- ============================================================
CREATE TABLE IF NOT EXISTS govt_spending.contract_awards (
    contract_id                 VARCHAR(50),
    agency_name                 VARCHAR(255),
    vendor_name                 VARCHAR(255),
    award_amount                DECIMAL(18,2),
    award_date                  DATE,
    contract_type               VARCHAR(100),
    naics_code                  VARCHAR(20),
    anomaly_score               INTEGER,
    flag_award_spike            INTEGER,
    flag_vendor_concentration   INTEGER,
    flag_modification_inflation INTEGER,
    agency_90day_avg            DECIMAL(18,2),
    vendor_agency_count         INTEGER,
    ingestion_timestamp         TIMESTAMP
)
DISTSTYLE KEY
DISTKEY(agency_name)
SORTKEY(award_date);

-- ============================================================
-- Why DISTKEY and SORTKEY choices matter
-- ============================================================
-- DISTKEY(agency_name):
--   Most queries filter and group by agency_name
--   This distributes data so all rows for the same agency
--   land on the same compute node — eliminates data shuffling
--   during GROUP BY and JOIN operations on agency_name
--
-- SORTKEY(award_date):
--   Most queries filter by date ranges
--   Redshift uses zone maps to skip irrelevant blocks
--   Queries like WHERE award_date BETWEEN x AND y
--   run dramatically faster with this sort key
-- ============================================================

-- ============================================================
-- Anomaly summary view
-- ============================================================
CREATE OR REPLACE VIEW govt_spending.anomaly_summary AS
SELECT
    agency_name,
    COUNT(*) as total_contracts,
    SUM(CASE WHEN anomaly_score >= 2 THEN 1 ELSE 0 END) as flagged_contracts,
    SUM(award_amount) as total_spend,
    SUM(CASE WHEN anomaly_score >= 2 THEN award_amount ELSE 0 END) as flagged_spend,
    ROUND(
        100.0 * SUM(CASE WHEN anomaly_score >= 2 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) as anomaly_rate_pct,
    AVG(award_amount) as avg_award_amount,
    MAX(award_amount) as max_award_amount
FROM govt_spending.contract_awards
GROUP BY agency_name
ORDER BY flagged_spend DESC;

-- ============================================================
-- Vendor concentration view
-- ============================================================
CREATE OR REPLACE VIEW govt_spending.vendor_concentration AS
SELECT
    vendor_name,
    COUNT(DISTINCT agency_name) as agency_count,
    COUNT(*) as total_contracts,
    SUM(award_amount) as total_awards,
    AVG(award_amount) as avg_award,
    MAX(anomaly_score) as max_anomaly_score
FROM govt_spending.contract_awards
WHERE anomaly_score >= 1
GROUP BY vendor_name
HAVING COUNT(DISTINCT agency_name) >= 3
ORDER BY total_awards DESC;