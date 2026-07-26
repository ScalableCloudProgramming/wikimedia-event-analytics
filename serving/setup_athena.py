# Create Athena DB + external tables over batch parquet on S3
# Run after a successful batch job: python serving/setup_athena.py
import os
import sys

import boto3
from pyathena import connect

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import config


def _cursor():
    return connect(
        s3_staging_dir=config.S3_ATHENA_OUT,
        region_name=config.AWS_REGION,
    ).cursor()


def ensure_database(cursor):
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {config.ATHENA_DB}")
    print(f"Database ready: {config.ATHENA_DB}")


def ensure_wiki_edits_table(cursor):
    location = config.s3_uri(config.S3_BATCH, f"{config.S3_BATCH_PREFIX}wiki_edits")
    cursor.execute(f"DROP TABLE IF EXISTS {config.ATHENA_DB}.{config.ATHENA_TABLE}")
    # SQL DDL string (not a docstring)
    cursor.execute(f"""
        CREATE EXTERNAL TABLE {config.ATHENA_DB}.{config.ATHENA_TABLE} (
            wiki  string,
            edits bigint
        )
        STORED AS PARQUET
        LOCATION '{location}'
    """)
    print(f"Table ready: {config.ATHENA_DB}.{config.ATHENA_TABLE} @ {location}")


def ensure_keywords_table(cursor):
    location = config.s3_uri(config.S3_BATCH, f"{config.S3_BATCH_PREFIX}keywords")
    cursor.execute(
        f"DROP TABLE IF EXISTS {config.ATHENA_DB}.{config.ATHENA_KEYWORDS_TABLE}"
    )
    cursor.execute(f"""
        CREATE EXTERNAL TABLE {config.ATHENA_DB}.{config.ATHENA_KEYWORDS_TABLE} (
            kw   string,
            freq bigint
        )
        STORED AS PARQUET
        LOCATION '{location}'
    """)
    print(f"Table ready: {config.ATHENA_DB}.{config.ATHENA_KEYWORDS_TABLE} @ {location}")


def ensure_hourly_table(cursor):
    location = config.s3_uri(config.S3_BATCH, f"{config.S3_BATCH_PREFIX}hourly")
    cursor.execute(
        f"DROP TABLE IF EXISTS {config.ATHENA_DB}.{config.ATHENA_HOURLY_TABLE}"
    )
    cursor.execute(f"""
        CREATE EXTERNAL TABLE {config.ATHENA_DB}.{config.ATHENA_HOURLY_TABLE} (
            hour  string,
            edits bigint
        )
        STORED AS PARQUET
        LOCATION '{location}'
    """)
    print(f"Table ready: {config.ATHENA_DB}.{config.ATHENA_HOURLY_TABLE} @ {location}")


def smoke_query(cursor):
    try:
        cursor.execute(
            f"SELECT wiki, edits FROM {config.ATHENA_DB}.{config.ATHENA_TABLE} "
            f"ORDER BY edits DESC LIMIT 5"
        )
        rows = cursor.fetchall()
        print(f"Smoke query rows: {rows}")
    except Exception as e:
        print(f"Smoke query skipped (batch output may be empty): {e}")


if __name__ == "__main__":
    # Check batch bucket is reachable before creating tables
    s3 = boto3.client("s3", region_name=config.AWS_REGION)
    try:
        s3.head_bucket(Bucket=config.S3_BATCH)
    except Exception as e:
        print(f"Warning: cannot access {config.S3_BATCH}: {e}")

    cur = _cursor()
    ensure_database(cur)
    ensure_wiki_edits_table(cur)
    ensure_keywords_table(cur)
    ensure_hourly_table(cur)
    smoke_query(cur)
    print("\nAthena setup complete.")
