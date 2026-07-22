import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from speed.consumer import SlidingWindow, should_count


def test_basic():
    w = SlidingWindow(window_s=60, bucket_s=5)
    w.add("enwiki")
    w.add("enwiki")
    w.add("dewiki")
    top = w.top(3)
    assert top[0] == ("enwiki", 2)
    assert top[1] == ("dewiki", 1)
    print("test_basic passed")


def test_eviction():
    w = SlidingWindow(window_s=1, bucket_s=1)
    w.add("old_item")
    time.sleep(1.5)
    w.add("new_item")
    top = w.top(5)
    assert all(item != "old_item" for item, _ in top)
    print("test_eviction passed")


def test_top_n():
    w = SlidingWindow(window_s=60, bucket_s=5)
    for i in range(8):
        w.add(f"wiki_{i}")
    assert len(w.top(3)) == 3
    print("test_top_n passed")


def test_should_count_filters_bots():
    assert should_count({"wiki": "enwiki", "type": "edit", "bot": False}) is True
    assert should_count({"wiki": "enwiki", "type": "edit", "bot": True}) is False
    assert should_count({"wiki": "enwiki", "type": "log"}) is False
    print("test_should_count_filters_bots passed")


def test_window_size():
    w = SlidingWindow(window_s=60, bucket_s=5)
    for _ in range(10):
        w.add("enwiki")
    assert w.size() == 10
    print("test_window_size passed")


def test_merge_logic():
    # local copy of serving merge to avoid AWS imports in unit path
    def merge(batch, speed, top_n=10):
        counts = {}
        for wiki, edits in batch:
            counts[wiki] = int(edits)
        for wiki, delta in speed:
            counts[wiki] = counts.get(wiki, 0) + int(delta)
        return sorted(counts.items(), key=lambda x: -x[1])[:top_n]

    batch = [("enwiki", 100), ("dewiki", 40)]
    speed = [("enwiki", 5), ("frwiki", 12)]
    merged = merge(batch, speed)
    assert merged[0] == ("enwiki", 105)
    assert ("frwiki", 12) in merged
    print("test_merge_logic passed")


if __name__ == "__main__":
    test_basic()
    test_eviction()
    test_top_n()
    test_should_count_filters_bots()
    test_window_size()
    test_merge_logic()
    print("all tests passed")
