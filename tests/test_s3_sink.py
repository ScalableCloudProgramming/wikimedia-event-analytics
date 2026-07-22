"""Unit tests for S3 raw sink buffer logic (no AWS calls)."""
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class FakeS3:
    def __init__(self):
        self.objects = []

    def put_object(self, **kwargs):
        self.objects.append(kwargs)


def test_flush_on_size():
    from ingestion.s3_sink import S3RawSink

    sink = S3RawSink(bucket="test-bucket", prefix="data/", flush_size=3, flush_seconds=999)
    sink.s3 = FakeS3()
    sink.add({"wiki": "enwiki", "n": 1})
    sink.add({"wiki": "enwiki", "n": 2})
    assert len(sink.s3.objects) == 0
    sink.add({"wiki": "dewiki", "n": 3})
    assert len(sink.s3.objects) == 1
    assert sink.records_written == 3
    print("test_flush_on_size passed")


def test_flush_on_close():
    from ingestion.s3_sink import S3RawSink

    sink = S3RawSink(bucket="test-bucket", prefix="data/", flush_size=100, flush_seconds=999)
    sink.s3 = FakeS3()
    sink.add({"wiki": "frwiki"})
    sink.close()
    assert len(sink.s3.objects) == 1
    body = sink.s3.objects[0]["Body"].decode()
    assert "frwiki" in body
    print("test_flush_on_close passed")


if __name__ == "__main__":
    test_flush_on_size()
    test_flush_on_close()
    print("all s3_sink tests passed")
