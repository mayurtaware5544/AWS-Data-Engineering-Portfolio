import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    sum,
    avg,
    round
)

from awsglue.utils import getResolvedOptions


# --------------------------------------------------
# Job Parameters
# --------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_PATH",
        "GOLD_PATH"
    ]
)


# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = SparkSession.builder \
    .appName(args["JOB_NAME"]) \
    .getOrCreate()


# --------------------------------------------------
# Read Silver Data
# --------------------------------------------------

silver_df = spark.read \
    .format("parquet") \
    .load(args["SILVER_PATH"])


# --------------------------------------------------
# Business Aggregation
# --------------------------------------------------

gold_df = silver_df.groupBy(
    "city"
).agg(

    count("transaction_id")
        .alias("total_transactions"),

    round(
        sum("transaction_amount"),
        2
    ).alias("total_transaction_amount"),

    round(
        avg("transaction_amount"),
        2
    ).alias("average_transaction_amount")
)


# --------------------------------------------------
# Write Gold Layer
# --------------------------------------------------

gold_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .save(args["GOLD_PATH"])


print("Gold layer aggregation completed successfully.")


spark.stop()
