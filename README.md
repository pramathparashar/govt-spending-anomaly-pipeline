# US Government Contract Spending Anomaly Detector

A real-time, end-to-end AWS data engineering pipeline that ingests live US federal government contract spending data, transforms it, detects anomalous spending patterns, and loads it into a data warehouse for querying and visualization.

---

## Key Finding

Detected a $22.4 billion Boeing NASA contract flagged as anomalous — scoring 2/3 on custom anomaly rules:

- Award amount exceeded NASA's 90-day rolling average by 3x (Award Spike)
- Award exceeded Boeing's historical average contract value by 1.5x (Modification Inflation)

---

## Architecture

```
USASpending.gov API (Free, Public, Real Data)
        |
Python Producer Script
        |
Amazon Kinesis Data Streams
        |
AWS Lambda (Schema Validation + Record Tagging)
        |
Amazon S3 — Raw Layer (JSON)
        |
AWS Glue Crawler -> Glue Data Catalog
        |
AWS Glue ETL Job (JSON -> Parquet + Anomaly Scoring)
        |
Amazon S3 — Processed Layer (Parquet)
        |
Amazon S3 — Curated Layer (Anomalies Only)
        |
Amazon Redshift Serverless (Data Warehouse)
        |
Amazon Athena (Ad-hoc Querying)
```

Cross-cutting services:
- AWS Step Functions — pipeline orchestration
- Amazon CloudWatch — monitoring and alerting
- AWS IAM — least-privilege roles per service
- AWS KMS — encryption at rest

---

## Business Problem

The US federal government awards $700B+ in contracts annually. Detecting anomalous or potentially wasteful spending manually is impossible at scale. This pipeline automates anomaly detection using three custom scoring rules applied to real-time data.

---

## Custom Anomaly Scoring Logic

Each contract receives an anomaly score from 0 to 3. Contracts scoring 2 or higher are flagged as high-risk and written to the curated S3 layer.

### Rule 1 — Award Spike

Flag contracts where the award amount exceeds 3x the same agency's 90-day rolling average.

    flag_award_spike = award_amount > (agency_90day_avg * 3)

### Rule 2 — Vendor Concentration

Flag vendors receiving awards from 3 or more different federal agencies within the same calendar week.

    flag_vendor_concentration = vendor_agency_count >= 3

### Rule 3 — Modification Inflation

Flag contracts where the award amount exceeds 1.5x the vendor's historical average award value.

    flag_modification_inflation = award_amount > (vendor_avg_award * 1.5)

---

## AWS Services Used

| Service | Purpose | Domain |
|---------|---------|--------|
| Amazon Kinesis | Real-time data streaming | Ingestion |
| AWS Lambda | Stream validation and S3 writes | Ingestion |
| Amazon S3 | Three-layer data lake (Raw/Processed/Curated) | Storage |
| AWS Glue Crawler | Automatic schema detection | Transformation |
| AWS Glue ETL | JSON to Parquet plus anomaly scoring | Transformation |
| Amazon Redshift Serverless | Data warehouse | Storage |
| Amazon Athena | Ad-hoc SQL queries on S3 | Operations |
| AWS Step Functions | Pipeline orchestration | Operations |
| Amazon CloudWatch | Monitoring and alerting | Operations |
| AWS IAM | Least-privilege security | Security |
| AWS KMS | Encryption at rest | Security |

---

## Repository Structure

```
govt-spending-anomaly-pipeline/
    ingestion/
        kinesis_producer.py      -- Hits USASpending.gov API, sends to Kinesis
    lambda/
        validator.py             -- Validates records, writes to S3 Raw
    glue/
        etl_transform.py         -- JSON to Parquet plus 3-rule anomaly scoring
    redshift/
        schema.sql               -- Table schema with DISTKEY and SORTKEY
    queries/
        anomaly_analysis.sql     -- 7 analytical queries
    stepfunctions/
        pipeline_definition.json -- State machine definition
    screenshots/                 -- Pipeline execution evidence
    README.md
```

---

## S3 Data Lake Structure

```
Raw Layer:    s3://bucket/raw/contracts/year=2026/month=05/day=05/shard=X/
Processed:    s3://bucket/processed/contracts/award_date=2026-05-05/
Curated:      s3://bucket/curated/contracts/anomalies/award_date=2026-05-05/
```

Lifecycle rule: Raw JSON moves to S3 Glacier after 90 days for cost optimization.

---

## Redshift Schema Design

```sql
CREATE TABLE govt_spending.contract_awards (
    contract_id                 VARCHAR(50),
    agency_name                 VARCHAR(255),
    vendor_name                 VARCHAR(255),
    award_amount                DOUBLE PRECISION,
    anomaly_score               INTEGER,
    flag_award_spike            INTEGER,
    flag_vendor_concentration   INTEGER,
    flag_modification_inflation INTEGER,
    agency_90day_avg            DOUBLE PRECISION,
    vendor_agency_count         BIGINT,
    ingestion_timestamp         TIMESTAMP
)
DISTKEY(agency_name)
SORTKEY(ingestion_timestamp);
```

DISTKEY(agency_name): Most queries filter and aggregate by agency. This ensures all records for the same agency land on the same compute node, eliminating data shuffling during GROUP BY operations.

SORTKEY(ingestion_timestamp): Enables zone map pruning for time-range queries, dramatically reducing data scanned.

---

## Pipeline Results

| Metric | Value |
|--------|-------|
| Records processed | 600+ |
| Anomalies detected | 6 flagged contracts |
| Largest anomaly | $22.4B Boeing/NASA contract |
| Data source | USASpending.gov (live federal data) |
| Pipeline latency | Less than 2 seconds from API to S3 |

---

## Screenshots

All 13 screenshots of the working pipeline are included in the screenshots/ folder:

- 01-cloudwatch-lambda-success.png — Lambda processing records successfully
- 02-producer-terminal-output.png — Producer sending 100 records to Kinesis
- 03-s3-raw-data-landing.png — Raw JSON files landing in S3
- 04-glue-crawler-completed.png — Crawler detecting schema automatically
- 05-glue-etl-job-running.png — ETL job executing
- 06-glue-data-catalog-table.png — Table registered in Data Catalog
- 07-glue-schema-detected.png — 18 columns detected automatically
- 08-glue-etl-job-succeeded.png — ETL job succeeded
- 09-s3-processed-parquet-data.png — Parquet files in processed layer
- 10-s3-curated-anomalies.png — Anomalous records in curated layer
- 11-redshift-data-loaded.png — 600 records loaded into Redshift
- 12-redshift-query-results.png — Agency spending breakdown query
- 13-redshift-anomaly-results.png — Boeing $22.4B anomaly detected

---

## How To Run

Prerequisites:
- AWS Account with appropriate IAM permissions
- Python 3.12 or higher
- AWS CLI configured

Setup:

    git clone https://github.com/pramathparashar/govt-spending-anomaly-pipeline.git
    cd govt-spending-anomaly-pipeline
    pip install boto3 requests
    aws configure
    python ingestion/kinesis_producer.py

---

## Debugging Challenges Solved

Three real production issues were debugged and resolved during this build:

1. USASpending.gov API returns empty award_date for some contracts — fixed with fallback to today's date in the transform function.

2. PySpark does not support countDistinct inside window functions — fixed by replacing with a groupBy and join approach for Rule 2.

3. Redshift COPY column mismatch — award_date stored as partition folder name not inside Parquet file — fixed by creating a second Glue crawler to inspect the actual Parquet schema before recreating the Redshift table with 13 columns.

---

## Estimated AWS Cost

| Service | Cost |
|---------|------|
| Kinesis On-demand | ~$0 when idle |
| Lambda | Free tier |
| S3 | Pennies |
| Glue ETL 2 DPU | ~$2 per run |
| Redshift Serverless 4 RPU | ~$0.36/hr when active |
| Total weekend build | ~$10 |

---

## Future Improvements

- Add SageMaker anomaly detection for ML-based scoring
- Build QuickSight dashboard for executive visualization
- Implement real-time alerting via SNS for high-score anomalies
- Add dbt for data transformation layer
- Expand to grants and loans data from USASpending.gov

---

## Certifications

Built to demonstrate skills covered in AWS Certified Data Engineer Associate (DEA-C01):

- Domain 1: Data Ingestion and Transformation — Kinesis, Lambda, Glue
- Domain 2: Data Store Management — S3, Redshift
- Domain 3: Data Operations and Support — Step Functions, Athena, CloudWatch
- Domain 4: Data Security and Governance — IAM, KMS

---

## Author

Pramath Parashar
Data Engineer | AWS Certified DEA-C01 | MS Data Science
LinkedIn: linkedin.com/in/pramathparashar
Currently building enterprise data systems at BHP Minerals