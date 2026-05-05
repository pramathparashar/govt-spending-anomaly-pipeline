import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from datetime import datetime, timedelta

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Glue and Spark contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
RAW_BUCKET = "govt-spending-pipeline-raw-pramath-352017689721-us-east-2-an"
PROCESSED_BUCKET = "govt-spending-processed-pramath-352017689721-us-east-2-an"
CURATED_BUCKET = "govt-spending-curated-pramath-352017689721-us-east-2-an"

RAW_PATH = f"s3://{RAW_BUCKET}/raw/contracts/"
PROCESSED_PATH = f"s3://{PROCESSED_BUCKET}/processed/contracts/"
CURATED_PATH = f"s3://{CURATED_BUCKET}/curated/contracts/anomalies/"

print("Starting Government Spending ETL Job...")
print(f"Reading from: {RAW_PATH}")

# ── EXTRACT ──────────────────────────────────────────────────────────────
# Read raw JSON files from S3
raw_df = spark.read.json(RAW_PATH)

print(f"Records read from raw layer: {raw_df.count()}")
raw_df.printSchema()

# ── TRANSFORM ─────────────────────────────────────────────────────────────

# Step 1 — Cast and clean columns
cleaned_df = raw_df.select(
    F.col("contract_id").cast("string"),
    F.col("agency_name").cast("string"),
    F.col("vendor_name").cast("string"),
    F.col("award_amount").cast("double"),
    F.to_date(F.col("award_date"), "yyyy-MM-dd").alias("award_date"),
    F.col("contract_type").cast("string"),
    F.col("naics_code").cast("string"),
    F.col("ingestion_timestamp").cast("timestamp"),
    F.col("source").cast("string")
).filter(
    F.col("contract_id").isNotNull() &
    F.col("award_amount").isNotNull() &
    (F.col("award_amount") > 0)
)

print(f"Records after cleaning: {cleaned_df.count()}")

# Step 2 — Add anomaly scoring

# Rule 1: Award Spike
# Flag contracts where award amount > 3x agency 90-day rolling average
window_90_days = Window.partitionBy("agency_name").orderBy(
    F.col("award_date").cast("long")
).rangeBetween(
    -90 * 86400,  # 90 days in seconds
    0
)

df_with_avg = cleaned_df.withColumn(
    "agency_90day_avg",
    F.avg("award_amount").over(window_90_days)
)

df_rule1 = df_with_avg.withColumn(
    "flag_award_spike",
    F.when(
        F.col("award_amount") > (F.col("agency_90day_avg") * 3),
        1
    ).otherwise(0)
)

# Rule 2: Vendor Concentration
# Flag vendors receiving awards from 3+ different agencies in same week
# Fix: Use a join approach instead of countDistinct window function

vendor_agency_weekly = df_rule1.groupBy(
    "vendor_name",
    F.weekofyear(F.col("award_date")).alias("week_num")
).agg(
    F.countDistinct("agency_name").alias("vendor_agency_count")
)

df_rule2 = df_rule1.join(
    vendor_agency_weekly,
    on=["vendor_name"],
    how="left"
).withColumn(
    "vendor_agency_count",
    F.coalesce(F.col("vendor_agency_count"), F.lit(0))
).withColumn(
    "flag_vendor_concentration",
    F.when(F.col("vendor_agency_count") >= 3, 1).otherwise(0)
)

# Rule 3: Modification Inflation
# Flag contracts where award amount > 1.5x the vendor's average award
window_vendor = Window.partitionBy("vendor_name")

df_rule3 = df_rule2.withColumn(
    "vendor_avg_award",
    F.avg("award_amount").over(window_vendor)
).withColumn(
    "flag_modification_inflation",
    F.when(
        F.col("award_amount") > (F.col("vendor_avg_award") * 1.5),
        1
    ).otherwise(0)
)

# Calculate total anomaly score (0-3)
df_scored = df_rule3.withColumn(
    "anomaly_score",
    (F.col("flag_award_spike") +
     F.col("flag_vendor_concentration") +
     F.col("flag_modification_inflation")).cast(IntegerType())
)

# Select final columns for processed layer
processed_df = df_scored.select(
    "contract_id",
    "agency_name",
    "vendor_name",
    "award_amount",
    "award_date",
    "contract_type",
    "naics_code",
    "anomaly_score",
    "flag_award_spike",
    "flag_vendor_concentration",
    "flag_modification_inflation",
    "agency_90day_avg",
    "vendor_agency_count",
    "ingestion_timestamp"
)

print(f"Records after anomaly scoring: {processed_df.count()}")
print("Anomaly score distribution:")
processed_df.groupBy("anomaly_score").count().orderBy("anomaly_score").show()

# ── LOAD ──────────────────────────────────────────────────────────────────

# Write to Processed layer as Parquet partitioned by date
print(f"Writing to processed layer: {PROCESSED_PATH}")
processed_df.write \
    .mode("append") \
    .partitionBy("award_date") \
    .parquet(PROCESSED_PATH)

print("Successfully written to processed layer")

# Write anomalies (score >= 2) to Curated layer
anomalies_df = processed_df.filter(F.col("anomaly_score") >= 2)

print(f"Anomalous records found: {anomalies_df.count()}")

if anomalies_df.count() > 0:
    print(f"Writing anomalies to curated layer: {CURATED_PATH}")
    anomalies_df.write \
        .mode("append") \
        .partitionBy("award_date") \
        .parquet(CURATED_PATH)
    print("Successfully written to curated layer")

# ── COMMIT ────────────────────────────────────────────────────────────────
job.commit()
print("ETL Job completed successfully")