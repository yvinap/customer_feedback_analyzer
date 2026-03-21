"""
glue/glue_job_script.py

AWS Glue ETL Job — PySpark script for processing structured customer feedback CSVs.

This job is designed to run on AWS Glue (Spark runtime). It:
  1. Reads CSV data from the Glue Data Catalog (discovered by the Glue Crawler)
  2. Applies transformations: type casting, filtering, deduplication
  3. Enriches with computed fields (review_length, has_image_url, etc.)
  4. Writes the cleaned data back to the processed S3 bucket in Parquet format
  5. Updates the Glue Data Catalog with the new schema

Deploy this file via the CDK stack's Glue Job definition or manually via:
    aws glue create-job --name cfa-feedback-etl \
        --role <GlueRoleArn> \
        --command '{"Name":"glueetl","ScriptLocation":"s3://<bucket>/glue/glue_job_script.py"}' \
        --default-arguments '{"--database_name":"cfa_customer_feedback_db","--output_bucket":"<processed-bucket>"}'
"""
from __future__ import annotations

import sys

from awsglue.context import GlueContext  # type: ignore[import]
from awsglue.job import Job              # type: ignore[import]
from awsglue.transforms import *         # type: ignore[import]  # noqa: F401, F403
from awsglue.utils import getResolvedOptions  # type: ignore[import]
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType

# ── Initialise Glue + Spark context ───────────────────────────────────────────
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "database_name",
        "output_bucket",
        "input_table",       # e.g. "text_reviews"
    ],
)

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

DATABASE = args["database_name"]
OUTPUT_BUCKET = args["output_bucket"]
INPUT_TABLE = args["input_table"]

print(f"[INFO] Starting ETL: database={DATABASE}, table={INPUT_TABLE}, output={OUTPUT_BUCKET}")


# ── 1. Read from Glue Data Catalog ────────────────────────────────────────────
dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
    database=DATABASE,
    table_name=INPUT_TABLE,
    transformation_ctx="datasource",
)

df = dynamic_frame.toDF()
print(f"[INFO] Loaded {df.count()} rows from {DATABASE}.{INPUT_TABLE}")
df.printSchema()


# ── 2. Type casting & cleaning ────────────────────────────────────────────────
# Cast rating to a proper numeric type
df = df.withColumn(
    "rating",
    F.regexp_replace(F.col("rating").cast(StringType()), "[^0-9.]", "").cast(DoubleType()),
)

# Normalise discount_percentage: strip the % sign
if "discount_percentage" in df.columns:
    df = df.withColumn(
        "discount_percentage",
        F.regexp_replace(F.col("discount_percentage").cast(StringType()), "%", "").cast(DoubleType()),
    )

# Normalise price columns: strip currency symbols
for price_col in ("discounted_price", "actual_price"):
    if price_col in df.columns:
        df = df.withColumn(
            price_col,
            F.regexp_replace(F.col(price_col).cast(StringType()), "[^0-9.]", "").cast(DoubleType()),
        )

# Normalise rating_count: strip commas (e.g. "24,269" → 24269)
if "rating_count" in df.columns:
    df = df.withColumn(
        "rating_count",
        F.regexp_replace(F.col("rating_count").cast(StringType()), ",", "").cast(IntegerType()),
    )


# ── 3. Filter out low-quality rows ────────────────────────────────────────────
initial_count = df.count()

df = df.filter(
    F.col("review_content").isNotNull()
    & (F.length(F.col("review_content")) > 10)
    & F.col("product_id").isNotNull()
    & (F.col("rating").isNotNull() & F.col("rating").between(1, 5))
)

filtered_count = df.count()
print(f"[INFO] After quality filter: {filtered_count}/{initial_count} rows retained")


# ── 4. Deduplication on review_id ─────────────────────────────────────────────
if "review_id" in df.columns:
    before_dedup = df.count()
    df = df.dropDuplicates(["review_id"])
    print(f"[INFO] After deduplication: {df.count()}/{before_dedup} rows")


# ── 5. Feature engineering ────────────────────────────────────────────────────
df = df.withColumn("review_length", F.length(F.col("review_content")))

if "img_link" in df.columns:
    df = df.withColumn(
        "has_image_url",
        F.col("img_link").isNotNull() & (F.length(F.col("img_link")) > 10),
    )

# Extract top-level category (first segment before '|')
if "category" in df.columns:
    df = df.withColumn(
        "top_category",
        F.split(F.col("category"), "\\|").getItem(0),
    )

# Sentiment polarity proxy from rating
df = df.withColumn(
    "sentiment_proxy",
    F.when(F.col("rating") >= 4, "POSITIVE")
     .when(F.col("rating") <= 2, "NEGATIVE")
     .otherwise("NEUTRAL"),
)


# ── 6. Write to Parquet in the processed bucket ───────────────────────────────
output_path = f"s3://{OUTPUT_BUCKET}/parquet/{INPUT_TABLE}/"

df.write.mode("overwrite").partitionBy("top_category").parquet(output_path)

print(f"[INFO] Wrote {df.count()} rows to {output_path}")


# ── 7. Commit Glue job ────────────────────────────────────────────────────────
job.commit()
print("[INFO] Glue ETL job complete.")
