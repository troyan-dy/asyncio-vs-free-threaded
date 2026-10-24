"""PROTOTYPE. ``def`` variant of the service (APP_SYNC=1).

The handler runs in anyio's thread pool (64 tokens); background work and the DB pool are the plain-thread
implementation from demo_app/rt_threads.
"""

from __future__ import annotations

import contextlib
import threading
from collections.abc import AsyncIterator

from anyio.to_thread import current_default_thread_limiter
from fastapi import FastAPI, Response
from fastapi import Request as FRequest

from demo_app import JSON_MEDIA_TYPE, build_app
from miniflask import Request
from rt_threads import ThreadRuntime

QUOTE_PATH = "/quote"
THREAD_TOKENS = 64  # anyio worker threads per loop


class SyncVariant:
    """miniflask App from demo_app behind FastAPI: lifespan starts its background work, quote() calls it."""

    def __init__(self) -> None:
        """Build the miniflask App on the thread runtime."""
        self._inner = build_app(ThreadRuntime())
        self._route = self._inner.routes[QUOTE_PATH]
        # Acquired (and never released) by the first lifespan of the process: it starts the background work.
        self._background = threading.Lock()

    @contextlib.asynccontextmanager
    async def lifespan(self, _app: FastAPI) -> AsyncIterator[None]:
        """Per loop: size the anyio thread pool; first loop: start the App's background work."""
        current_default_thread_limiter().total_tokens = THREAD_TOKENS  # per loop
        if self._background.acquire(blocking=False):
            self._start_background()
        yield

    def quote(self, request: FRequest) -> Response:
        """GET /quote: the demo_app handler, run by FastAPI in the thread pool."""
        body = self._route(Request(QUOTE_PATH, dict(request.query_params)))
        return Response(body, media_type=JSON_MEDIA_TYPE)

    def _start_background(self) -> None:
        for start in self._inner.on_start:
            start()
        for loop in self._inner.background:
            threading.Thread(target=loop, daemon=True).start()
