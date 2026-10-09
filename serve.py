"""PROTOTYPE. python serve.py --rt greenlet|threads --workers N --port 8080"""

import argparse
import os
import sys

from demo_app import build_app


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rt", choices=["greenlet", "threads"], required=True)
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--port", type=int, default=8080)
    a = p.parse_args()

    if a.rt == "greenlet":
        from gort import GreenletRuntime

        rt = GreenletRuntime(a.workers)
    else:
        from rt_threads import ThreadRuntime

        rt = ThreadRuntime()

    app = build_app(rt)
    print(f"serving rt={a.rt} workers={a.workers} gil={sys._is_gil_enabled()} port={a.port}", flush=True)  # noqa: SLF001
    rt.go(app.run, os.environ.get("BIND_HOST", "127.0.0.1"), a.port).get()


if __name__ == "__main__":
    main()
