import sys, os, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from speed.consumer import SlidingWindow


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


if __name__ == "__main__":
    test_basic()
    test_eviction()
    test_top_n()
    print("all tests passed")
