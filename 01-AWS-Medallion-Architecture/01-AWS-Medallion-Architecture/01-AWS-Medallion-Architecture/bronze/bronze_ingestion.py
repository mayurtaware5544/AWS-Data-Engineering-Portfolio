from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from awsglue.utils import getResolvedOptions


args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SOURCE_PATH",
        "BRONZE_PATH"
    ]
)

spark = SparkSession.builder \
    .appName(args["JOB_NAME"]) \
    .getOrCreate()


df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(args["SOURCE_PATH"])


bronze_df = df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)


bronze_df.write \
    .mode("append") \
    .format("parquet") \
    .save(args["BRONZE_PATH"])


print("Bronze layer ingestion completed successfully.")

spark.stop()
