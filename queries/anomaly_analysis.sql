-- ============================================================
-- Government Contract Spending Anomaly Analysis Queries
-- Run these in Amazon Athena against S3 processed layer
-- ============================================================

-- Query 1: Top 10 agencies by anomalous contract volume
SELECT 
    agency_name,
    COUNT(*) as anomaly_count,
    SUM(award_amount) as total_flagged_spend,
    AVG(award_amount) as avg_flagged_amount,
    MAX(award_amount) as max_flagged_amount
FROM contracts
WHERE anomaly_score >= 2
GROUP BY agency_name
ORDER BY total_flagged_spend DESC
LIMIT 10;

-- ============================================================

-- Query 2: Vendor concentration anomalies
-- Vendors receiving awards from 3+ different agencies
SELECT 
    vendor_name,
    COUNT(DISTINCT agency_name) as agency_count,
    COUNT(*) as total_contracts,
    SUM(award_amount) as total_awards,
    AVG(award_amount) as avg_award
FROM contracts
WHERE anomaly_score >= 1
GROUP BY vendor_name
HAVING COUNT(DISTINCT agency_name) >= 3
ORDER BY total_awards DESC;

-- ============================================================

-- Query 3: Anomaly rate trend by agency over rolling 30 days
SELECT 
    agency_name,
    award_date,
    COUNT(*) as total_contracts,
    SUM(CASE WHEN anomaly_score >= 2 THEN 1 ELSE 0 END) as flagged_contracts,
    ROUND(
        100.0 * SUM(CASE WHEN anomaly_score >= 2 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) as anomaly_rate_pct,
    AVG(award_amount) as avg_award_amount
FROM contracts
GROUP BY agency_name, award_date
ORDER BY agency_name, award_date;

-- ============================================================

-- Query 4: Award spike detection
-- Contracts flagged for award spike anomaly
SELECT
    contract_id,
    agency_name,
    vendor_name,
    award_amount,
    agency_90day_avg,
    ROUND(award_amount / NULLIF(agency_90day_avg, 0), 2) as spike_ratio,
    award_date,
    anomaly_score
FROM contracts
WHERE flag_award_spike = 1
ORDER BY spike_ratio DESC
LIMIT 20;

-- ============================================================

-- Query 5: High value anomalous contracts
-- Top contracts by award amount with high anomaly scores
SELECT
    contract_id,
    agency_name,
    vendor_name,
    award_amount,
    award_date,
    contract_type,
    anomaly_score,
    flag_award_spike,
    flag_vendor_concentration,
    flag_modification_inflation
FROM contracts
WHERE anomaly_score >= 2
    AND award_amount > 1000000
ORDER BY award_amount DESC
LIMIT 25;

-- ============================================================

-- Query 6: Daily pipeline health check
-- How many records processed each day
SELECT
    award_date,
    COUNT(*) as total_records,
    SUM(CASE WHEN anomaly_score = 0 THEN 1 ELSE 0 END) as clean_records,
    SUM(CASE WHEN anomaly_score = 1 THEN 1 ELSE 0 END) as low_risk,
    SUM(CASE WHEN anomaly_score = 2 THEN 1 ELSE 0 END) as medium_risk,
    SUM(CASE WHEN anomaly_score = 3 THEN 1 ELSE 0 END) as high_risk,
    SUM(award_amount) as total_spend
FROM contracts
GROUP BY award_date
ORDER BY award_date DESC;

-- ============================================================

-- Query 7: NAICS code anomaly distribution
-- Which industries have most anomalous spending
SELECT
    naics_code,
    COUNT(*) as total_contracts,
    SUM(CASE WHEN anomaly_score >= 2 THEN 1 ELSE 0 END) as anomalous_contracts,
    SUM(award_amount) as total_spend,
    ROUND(
        100.0 * SUM(CASE WHEN anomaly_score >= 2 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) as anomaly_rate_pct
FROM contracts
WHERE naics_code IS NOT NULL
GROUP BY naics_code
ORDER BY anomaly_rate_pct DESC
LIMIT 15;