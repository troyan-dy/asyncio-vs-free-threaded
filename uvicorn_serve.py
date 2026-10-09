"""PROTOTYPE. Launch fastapi_app under uvicorn in one of two shapes:

  --loops N    N event loops in N threads of ONE process, all accepting on one shared listening socket
               (only makes sense on the free-threaded build)
  --workers N  N worker processes (uvicorn's standard multiprocess mode)

python uvicorn_serve.py --loops 8 --loop uvloop --http httptools
"""

import argparse
import os
import socket
import sys
import threading

import uvicorn


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--loops", type=int, default=1)
    p.add_argument("--workers", type=int, default=0)
    p.add_argument("--loop", default="uvloop")
    p.add_argument("--http", default="httptools")
    p.add_argument("--port", type=int, default=8080)
    a = p.parse_args()

    bind = os.environ.get("BIND_HOST", "127.0.0.1")
    common = dict(loop=a.loop, http=a.http, lifespan="on", log_level="warning", access_log=False)
    print(
        f"serving fastapi loops={a.loops} workers={a.workers} {a.loop}/{a.http} gil={sys._is_gil_enabled()}",  # noqa: SLF001
        flush=True,
    )

    reuseport = sys.platform == "linux"  # kernel balances connections across SO_REUSEPORT sockets

    def listen_socket() -> socket.socket:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if reuseport:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        s.bind((bind, a.port))
        s.listen(1024)
        return s

    if a.workers:
        # fork before any event loop exists; each worker owns an SO_REUSEPORT socket
        # (uvicorn's own --workers shares one socket; asyncio's accept loop then grabs whole bursts
        #  of connections into one worker, which skews the comparison)
        for _ in range(a.workers):
            if os.fork() == 0:
                from fastapi_app import app

                uvicorn.Server(uvicorn.Config(app, **common)).run(sockets=[listen_socket()])
                os._exit(0)
        while True:
            os.wait()

    shared = None if reuseport else listen_socket()

    def run_loop() -> None:
        from fastapi_app import app

        uvicorn.Server(uvicorn.Config(app, **common)).run(sockets=[shared or listen_socket()])

    threads = [threading.Thread(target=run_loop, name=f"loop-{i}", daemon=True) for i in range(a.loops)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


if __name__ == "__main__":
    main()
