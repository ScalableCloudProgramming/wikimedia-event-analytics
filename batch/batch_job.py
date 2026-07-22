# Batch layer: full-history Spark aggregates over S3 raw JSON
# Outputs: wiki_edits, keywords, hourly, edit_types, namespaces
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

S3_IN = os.getenv("S3_RAW", "s3://wikimedia-pipeline-raw/data/")
if not S3_IN.startswith("s3://"):
    S3_IN = f"s3://{S3_IN}/data/"
S3_OUT = os.getenv("S3_BATCH", "s3://wikimedia-pipeline-batch/output/")
if not S3_OUT.startswith("s3://"):
    S3_OUT = f"s3://{S3_OUT}/output/"
if not S3_OUT.endswith("/"):
    S3_OUT += "/"
TOP_N = int(os.getenv("TOP_N", "20"))
EXCLUDE_BOTS = os.getenv("EXCLUDE_BOTS", "true").lower() in ("1", "true", "yes")

STOP = {
    "the", "a", "an", "of", "and", "in", "to", "is", "it", "for",
    "on", "at", "by", "with", "from", "as", "or", "that", "this",
}


@F.udf(returnType=ArrayType(StringType()))
def keywords(title):
    if not title:
        return []
    return [w for w in re.findall(r"[a-zA-Z]{3,}", title.lower()) if w not in STOP]


def _has_col(df, name):
    return name in df.columns


def run(spark):
    df = spark.read.json(S3_IN).cache()
    total = df.count()
    print(f"Records: {total}")

    work = df
    if EXCLUDE_BOTS and _has_col(df, "bot"):
        work = df.filter((F.col("bot") == False) | F.col("bot").isNull())  # noqa: E712
        print(f"After bot filter: {work.count()}")

    # top wikis by edit count
    (work.groupBy("wiki")
         .agg(F.count("*").alias("edits"))
         .orderBy(F.desc("edits"))
         .limit(TOP_N)
         .write.mode("overwrite")
         .parquet(f"{S3_OUT}wiki_edits/"))

    # keyword frequency from page titles
    if _has_col(work, "title"):
        (work.select(F.explode(keywords("title")).alias("kw"))
             .groupBy("kw")
             .agg(F.count("*").alias("freq"))
             .orderBy(F.desc("freq"))
             .limit(TOP_N)
             .write.mode("overwrite")
             .parquet(f"{S3_OUT}keywords/"))

    # hourly edit volume
    if _has_col(work, "timestamp"):
        (work.withColumn("hour", F.from_unixtime("timestamp", "yyyy-MM-dd HH"))
             .groupBy("hour")
             .agg(F.count("*").alias("edits"))
             .orderBy("hour")
             .write.mode("overwrite")
             .parquet(f"{S3_OUT}hourly/"))

    # edit type breakdown
    if _has_col(work, "type"):
        (work.groupBy("type")
             .agg(F.count("*").alias("edits"))
             .orderBy(F.desc("edits"))
             .write.mode("overwrite")
             .parquet(f"{S3_OUT}edit_types/"))

    # namespace summary
    if _has_col(work, "namespace"):
        (work.groupBy("namespace")
             .agg(F.count("*").alias("edits"))
             .orderBy(F.desc("edits"))
             .limit(TOP_N)
             .write.mode("overwrite")
             .parquet(f"{S3_OUT}namespaces/"))

    df.unpersist()
    print(f"Batch output: {S3_OUT}")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("wikimedia-batch")
        .config("spark.sql.shuffle.partitions", "100")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run(spark)
    spark.stop()
