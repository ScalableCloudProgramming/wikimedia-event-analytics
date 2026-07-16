"""
Buffered dual-write of raw events to S3 so the batch layer has full-history input.

Flushes JSONL objects under s3://{S3_RAW}/{S3_RAW_PREFIX}yyyy/mm/dd/HH/
when either the buffer size or time threshold is hit.
"""
import json
import time
import uuid
from datetime import datetime, timezone

import boto3

import config


class S3RawSink:
    def __init__(
        self,
        bucket=None,
        prefix=None,
        flush_size=None,
        flush_seconds=None,
        region=None,
    ):
        self.bucket = bucket or config.S3_RAW
        self.prefix = (prefix or config.S3_RAW_PREFIX).lstrip("/")
        if self.prefix and not self.prefix.endswith("/"):
            self.prefix += "/"
        self.flush_size = flush_size or config.S3_RAW_FLUSH_SIZE
        self.flush_seconds = flush_seconds or config.S3_RAW_FLUSH_SECONDS
        self.s3 = boto3.client("s3", region_name=region or config.AWS_REGION)
        self._buf = []
        self._last_flush = time.time()
        self.records_written = 0
        self.objects_written = 0

    def add(self, record: dict):
        self._buf.append(record)
        now = time.time()
        if len(self._buf) >= self.flush_size or (now - self._last_flush) >= self.flush_seconds:
            self.flush()

    def flush(self):
        if not self._buf:
            return
        now = datetime.now(timezone.utc)
        key = (
            f"{self.prefix}{now.strftime('%Y/%m/%d/%H')}/"
            f"events-{now.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}.jsonl"
        )
        body = "\n".join(json.dumps(r, default=str) for r in self._buf) + "\n"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        n = len(self._buf)
        self.records_written += n
        self.objects_written += 1
        self._buf.clear()
        self._last_flush = time.time()
        print(f"  s3://{self.bucket}/{key} ({n} records)")

    def close(self):
        self.flush()
