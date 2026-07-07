from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

S3_IN  = os.getenv("S3_RAW",   "s3://wikimedia-pipeline-raw/data/")
S3_OUT = os.getenv("S3_BATCH", "s3://wikimedia-pipeline-batch/output/")
TOP_N  = int(os.getenv("TOP_N", "20"))


def run(spark):
    df = spark.read.json(S3_IN)
    print(f"Records: {df.count()}")
    (df.groupBy("wiki")
       .agg(F.count("*").alias("edits"))
       .orderBy(F.desc("edits"))
       .limit(TOP_N)
       .write.mode("overwrite")
       .parquet(f"{S3_OUT}wiki_edits/"))
    print(f"Done: {S3_OUT}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("wikimedia-batch").getOrCreate()
    run(spark)
    spark.stop()
