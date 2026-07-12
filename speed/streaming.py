from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType
import os

STREAM   = os.getenv("KINESIS_STREAM", "wikimedia-stream")
REGION   = os.getenv("AWS_REGION", "eu-west-1")
S3_CKPT  = os.getenv("S3_BATCH", "s3://wikimedia-pipeline-batch") + "/checkpoints/speed/"
WIN_DUR  = "5 minutes"
WIN_SLIDE = "1 minute"

schema = StructType([
    StructField("wiki",      StringType(), True),
    StructField("title",     StringType(), True),
    StructField("timestamp", LongType(),   True),
    StructField("user",      StringType(), True),
    StructField("type",      StringType(), True),
])


def run(spark):
    raw = (spark.readStream
               .format("aws-kinesis")
               .option("kinesis.streamName", STREAM)
               .option("kinesis.region", REGION)
               .option("kinesis.startingposition", "LATEST")
               .load())

    parsed = (raw
              .select(
                  F.from_json(F.col("data").cast("string"), schema).alias("e"),
                  F.col("approximateArrivalTimestamp").alias("event_time"))
              .select("e.*", "event_time"))

    # sliding window: top wikis in last 5 min, updated every minute
    windowed = (parsed
                .withWatermark("event_time", "1 minute")
                .groupBy(
                    F.window("event_time", WIN_DUR, WIN_SLIDE),
                    F.col("wiki"))
                .agg(F.count("*").alias("edits")))

    query = (windowed.writeStream
                     .outputMode("complete")
                     .format("console")
                     .option("truncate", False)
                     .option("checkpointLocation", S3_CKPT)
                     .trigger(processingTime="30 seconds")
                     .start())

    query.awaitTermination()


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("wikimedia-streaming")
             .getOrCreate())
    run(spark)
