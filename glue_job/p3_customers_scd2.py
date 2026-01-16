import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

args = getResolvedOptions(sys.argv, ['run_date'])
run_date = args['run_date']  # example: 2026-01-02

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

bucket = "surya-project3-scd"

raw_day1_path = f"s3://{bucket}/raw/customers/customers_day1.csv"
raw_day2_path = f"s3://{bucket}/raw/customers/customers_day2.csv"
silver_path   = f"s3://{bucket}/silver/customers_scd2/"

# Define schema (avoid inference surprises)
schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_unique_id", StringType(), True),
    StructField("customer_zip_code_prefix", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
])

def read_csv(path):
    return (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(path)
    )

# --- Step A: Build initial dimension from Day 1 (first run) ---
day1 = read_csv(raw_day1_path).select(
    "customer_id", "customer_unique_id", "customer_city", "customer_state"
).dropDuplicates(["customer_id"])

dim_day1 = (
    day1
    .withColumn("start_date", F.lit("2026-01-01"))
    .withColumn("end_date", F.lit(None).cast("string"))
    .withColumn("is_current", F.lit("true"))
)

# --- Step B: Read current silver if exists; otherwise start from day1 dimension ---
# Use try/except pattern: if path doesn't exist, initialize with day1
try:
    existing = spark.read.parquet(silver_path)
    base_dim = existing
except Exception:
    base_dim = dim_day1

# --- Step C: Apply Day 2 changes as SCD Type 2 ---
day2 = read_csv(raw_day2_path).select(
    "customer_id", "customer_unique_id", "customer_city", "customer_state"
).dropDuplicates(["customer_id"])

current_dim = base_dim.filter(F.col("is_current") == "true")

# Detect changed customers (compare city/state)
changes = (
    day2.alias("n")
    .join(current_dim.alias("c"), on="customer_id", how="inner")
    .where(
        (F.col("n.customer_city") != F.col("c.customer_city")) |
        (F.col("n.customer_state") != F.col("c.customer_state"))
    )
    .select("customer_id", "n.customer_unique_id", "n.customer_city", "n.customer_state")
)

# Expire old records
expired = (
    current_dim.alias("c")
    .join(changes.select("customer_id").alias("chg"), on="customer_id", how="inner")
    .withColumn("end_date", F.lit(run_date))
    .withColumn("is_current", F.lit("false"))
)

# New current records
new_rows = (
    changes
    .withColumn("start_date", F.lit(run_date))
    .withColumn("end_date", F.lit(None).cast("string"))
    .withColumn("is_current", F.lit("true"))
)

# Keep untouched records (still current + all historical not current)
untouched = base_dim.join(changes.select("customer_id"), on="customer_id", how="left_anti")

final_dim = untouched.unionByName(expired).unionByName(new_rows)

# Write back (overwrite the whole dimension table)
final_dim.write.mode("overwrite").parquet(silver_path)

print(f"✅ SCD Type 2 applied successfully for run_date={run_date}")
