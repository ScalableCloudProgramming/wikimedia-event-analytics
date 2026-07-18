"""Compatibility wrapper — use batch/submit.py."""
from submit import upload, submit, wait, step_runtime_seconds

if __name__ == "__main__":
    upload()
    sid = submit()
    print(f"Step: {sid}")
    state, status = wait(sid)
    rt = step_runtime_seconds(status)
    print(f"Done: {state}" + (f" runtime={rt:.1f}s" if rt else ""))
