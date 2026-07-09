from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
import re, os

S3_IN  = os.getenv("S3_RAW",   "s3://wikimedia-pipeline-raw/data/")
S3_OUT = os.getenv("S3_BATCH", "s3://wikimedia-pipeline-batch/output/")
TOP_N  = int(os.getenv("TOP_N", "20"))

STOP = {"the","a","an","of","and","in","to","is","it","for","on","at","by","with"}


@F.udf(returnType=ArrayType(StringType()))
def keywords(title):
    if not title:
        return []
    return [w for w in re.findall(r"[a-zA-Z]{3,}", title.lower()) if w not in STOP]


def run(spark):
    df = spark.read.json(S3_IN).cache()
    print(f"Records: {df.count()}")

    (df.groupBy("wiki")
       .agg(F.count("*").alias("edits"))
       .orderBy(F.desc("edits"))
       .limit(TOP_N)
       .write.mode("overwrite")
       .parquet(f"{S3_OUT}wiki_edits/"))

    (df.select(F.explode(keywords("title")).alias("kw"))
       .groupBy("kw")
       .agg(F.count("*").alias("freq"))
       .orderBy(F.desc("freq"))
       .limit(TOP_N)
       .write.mode("overwrite")
       .parquet(f"{S3_OUT}keywords/"))

    (df.withColumn("hour", F.from_unixtime("timestamp", "yyyy-MM-dd HH"))
       .groupBy("hour")
       .agg(F.count("*").alias("edits"))
       .orderBy("hour")
       .write.mode("overwrite")
       .parquet(f"{S3_OUT}hourly/"))

    df.unpersist()
    print(f"Batch output: {S3_OUT}")


if __name__ == "__main__":
    spark = (SparkSession.builder
             .appName("wikimedia-batch")
             .config("spark.sql.shuffle.partitions", "100")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    run(spark)
    spark.stop()
