"""
Compatibility entrypoint — delegates to producer.py.
Prefer: python ingestion/producer.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from producer import wikimedia, usgs, replay
import config

if __name__ == "__main__":
    sources = {"wikimedia": wikimedia, "usgs": usgs, "replay": replay}
    if config.DATA_SOURCE not in sources:
        raise ValueError(f"Unknown DATA_SOURCE: {config.DATA_SOURCE}")
    sources[config.DATA_SOURCE]()
