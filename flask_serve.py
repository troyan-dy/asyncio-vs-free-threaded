"""PROTOTYPE. Reference point: the same service on real Flask served by waitress (thread pool,
keep-alive; --server werkzeug for the dev server). Background work = plain daemon threads.
python flask_serve.py --port 8080 --threads 64
"""

import argparse
import os
import sys
import threading

from flask import Flask, request
from werkzeug.serving import make_server

from demo_app import build_app
from rt_threads import ThreadRuntime


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--server", choices=["waitress", "werkzeug"], default="waitress")
    p.add_argument("--threads", type=int, default=64)
    a = p.parse_args()
    BIND = os.environ.get("BIND_HOST", "127.0.0.1")

    rt = ThreadRuntime()
    inner = build_app(rt)  # reuse handlers/background tasks; only the HTTP layer differs
    flask_app = Flask("demo")

    for path, handler in inner.routes.items():

        def make_view(h):  # noqa: ANN001
            def view():  # noqa: ANN202
                from miniflask import Request

                body = h(Request(request.path, {k: v for k, v in request.args.items()}))
                return flask_app.response_class(body, mimetype="application/json")

            return view

        flask_app.add_url_rule(path, endpoint=path, view_func=make_view(handler))

    for fn in inner.on_start:
        fn()
    for loop in inner.background:
        threading.Thread(target=loop, daemon=True).start()

    print(f"serving flask/{a.server} threads={a.threads} gil={sys._is_gil_enabled()} port={a.port}", flush=True)  # noqa: SLF001
    if a.server == "werkzeug":
        # NB: werkzeug dev server answers Connection: close on every request (no keep-alive).
        make_server(BIND, a.port, flask_app, threaded=True).serve_forever()
    else:
        from waitress import serve

        serve(flask_app, host=BIND, port=a.port, threads=a.threads, backlog=1024, _quiet=True)


if __name__ == "__main__":
    main()
