"""Shim — canonical entrypoint is infra/teardown.py."""
import runpy
import os
runpy.run_path(os.path.join(os.path.dirname(__file__), "..", "infra", "teardown.py"), run_name="__main__")
