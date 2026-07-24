# Batch layer: full-history Spark aggregates over S3 raw JSON
# Outputs: wiki_edits, keywords, hourly, edit_types, namespaces
import os
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, StringType, StructType, StructField, LongType, BooleanType, IntegerType,
)

def _norm_in(path):
    if not path.startswith("s3://"):
        path = f"s3://{path}"
    if not path.rstrip("/").endswith("data"):
        if "/data" not in path:
            path = path.rstrip("/") + "/data/"
    if not path.endswith("/"):
        path += "/"
    return path


def _norm_out(path):
    if not path.startswith("s3://"):
        path = f"s3://{path}"
    if not path.rstrip("/").endswith("output"):
        if "/output" not in path:
            path = path.rstrip("/") + "/output/"
    if not path.endswith("/"):
        path += "/"
    return path


S3_IN = _norm_in(os.getenv("S3_RAW", "s3://wikimedia-analytics-raw/data/"))
S3_OUT = _norm_out(os.getenv("S3_BATCH", "s3://wikimedia-analytics-batch/output/"))
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


# Explicit schema — mixed fan-out / live events break Spark auto-inference
EVENT_SCHEMA = StructType([
    StructField("wiki", StringType(), True),
    StructField("title", StringType(), True),
    StructField("type", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user", StringType(), True),
    StructField("namespace", IntegerType(), True),
    StructField("id", StringType(), True),
])


def run(spark):
    # path must include files; recursive JSONL under data/
    print(f"Reading JSONL from {S3_IN}")
    # recursiveFileLookup: data is under data/yyyy/mm/dd/HH/*.jsonl
    df = (
        spark.read
        .schema(EVENT_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("recursiveFileLookup", "true")
        .json(S3_IN)
        .cache()
    )

    total = df.count()
    print(f"Records: {total}")
    if total == 0:
        raise SystemExit(f"No records under {S3_IN}")

    work = df
    if EXCLUDE_BOTS and _has_col(df, "bot"):
        work = df.filter((F.col("bot") == False) | F.col("bot").isNull())  # noqa: E712
        print(f"After bot filter: {work.count()}")

    work = work.filter(F.col("wiki").isNotNull())

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
    import sys
    # Optional CLI: batch_job.py <s3_in> <s3_out> [top_n]
    if len(sys.argv) >= 2:
        S3_IN = _norm_in(sys.argv[1])
    if len(sys.argv) >= 3:
        S3_OUT = _norm_out(sys.argv[2])
    if len(sys.argv) >= 4:
        TOP_N = int(sys.argv[3])
    print(f"S3_IN={S3_IN} S3_OUT={S3_OUT} TOP_N={TOP_N}")

    spark = (
        SparkSession.builder
        .appName("wikimedia-batch")
        .config("spark.sql.shuffle.partitions", "100")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run(spark)
    spark.stop()
