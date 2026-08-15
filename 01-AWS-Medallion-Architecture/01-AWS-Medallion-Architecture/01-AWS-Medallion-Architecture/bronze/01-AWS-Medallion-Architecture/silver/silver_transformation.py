from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_timestamp,
    when
)

from awsglue.utils import getResolvedOptions


# --------------------------------------------------
# Job Parameters
# --------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "BRONZE_PATH",
        "SILVER_PATH"
    ]
)


# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = SparkSession.builder \
    .appName(args["JOB_NAME"]) \
    .getOrCreate()


# --------------------------------------------------
# Read Bronze Data
# --------------------------------------------------

bronze_df = spark.read \
    .format("parquet") \
    .load(args["BRONZE_PATH"])


# --------------------------------------------------
# Remove Duplicate Records
# --------------------------------------------------

df = bronze_df.dropDuplicates(
    ["transaction_id"]
)


# --------------------------------------------------
# Clean String Columns
# --------------------------------------------------

df = df.withColumn(
    "customer_name",
    trim(col("customer_name"))
)

df = df.withColumn(
    "city",
    upper(trim(col("city")))
)


# --------------------------------------------------
# Convert Data Types
# --------------------------------------------------

df = df.withColumn(
    "transaction_amount",
    col("transaction_amount").cast("double")
)

df = df.withColumn(
    "transaction_timestamp",
    to_timestamp(col("transaction_timestamp"))
)


# --------------------------------------------------
# Handle Null Values
# --------------------------------------------------

df = df.fillna({
    "city": "UNKNOWN"
})


# --------------------------------------------------
# Create Transaction Status
# --------------------------------------------------

df = df.withColumn(
    "transaction_status",
    when(
        col("transaction_amount") > 0,
        "VALID"
    ).otherwise("INVALID")
)


# --------------------------------------------------
# Keep Valid Records
# --------------------------------------------------

silver_df = df.filter(
    col("transaction_status") == "VALID"
)


# --------------------------------------------------
# Write Silver Layer
# --------------------------------------------------

silver_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("city") \
    .save(args["SILVER_PATH"])


print("Silver layer transformation completed successfully.")


spark.stop()
