"""Shim — canonical entrypoint is infra/setup.py."""
import runpy
import os
runpy.run_path(os.path.join(os.path.dirname(__file__), "..", "infra", "setup.py"), run_name="__main__")
