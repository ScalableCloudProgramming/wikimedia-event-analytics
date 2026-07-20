"""
Spark Structured Streaming speed layer — sliding window over Kinesis.

Writes windowed wiki edit counts to S3 parquet (and console) so results
survive beyond the driver process.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType

STREAM = os.getenv("KINESIS_STREAM", "wikimedia-stream")
REGION = os.getenv("AWS_REGION", "eu-west-1")
S3_BATCH = os.getenv("S3_BATCH", "wikimedia-pipeline-batch")
if S3_BATCH.startswith("s3://"):
    S3_BASE = S3_BATCH.rstrip("/")
else:
    S3_BASE = f"s3://{S3_BATCH}"
S3_CKPT = f"{S3_BASE}/checkpoints/speed/"
S3_OUT = f"{S3_BASE}/speed/windowed/"
WIN_DUR = os.getenv("WIN_DUR", "5 minutes")
WIN_SLIDE = os.getenv("WIN_SLIDE", "1 minute")

schema = StructType([
    StructField("wiki", StringType(), True),
    StructField("title", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user", StringType(), True),
    StructField("type", StringType(), True),
    StructField("bot", BooleanType(), True),
])


def run(spark):
    raw = (
        spark.readStream
        .format("aws-kinesis")
        .option("kinesis.streamName", STREAM)
        .option("kinesis.region", REGION)
        .option("kinesis.startingposition", "LATEST")
        .load()
    )

    parsed = (
        raw
        .select(
            F.from_json(F.col("data").cast("string"), schema).alias("e"),
            F.col("approximateArrivalTimestamp").alias("event_time"),
        )
        .select("e.*", "event_time")
        .filter(
            (F.col("bot").isNull() | (F.col("bot") == False))  # noqa: E712
            & (F.col("wiki").isNotNull())
        )
    )

    windowed = (
        parsed
        .withWatermark("event_time", "1 minute")
        .groupBy(F.window("event_time", WIN_DUR, WIN_SLIDE), F.col("wiki"))
        .agg(F.count("*").alias("edits"))
    )

    # Persist to S3 for serving / inspection
    q_s3 = (
        windowed.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", S3_OUT)
        .option("checkpointLocation", S3_CKPT)
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Also print for live demo
    q_console = (
        windowed.writeStream
        .outputMode("complete")
        .format("console")
        .option("truncate", False)
        .option("checkpointLocation", f"{S3_CKPT}console/")
        .trigger(processingTime="30 seconds")
        .start()
    )

    print(f"Streaming speed layer → {S3_OUT}")
    q_s3.awaitTermination()
    q_console.awaitTermination()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("wikimedia-streaming").getOrCreate()
    run(spark)
