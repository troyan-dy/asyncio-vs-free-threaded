"""PROTOTYPE. Background tasks of the async variant. They update ``state``, which is shared by every
loop/thread in this process (not across worker processes).
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
import traceback
from collections.abc import Awaitable, Callable

from async_pool import AsyncPool
from backend_protocol import STREAM_MSG, STREAM_MSG_SIZE
from demo_app import CONFIG_PATH, CONFIG_PERIOD, STREAM_ADDR, ConfigWatcher, State

STREAM_RECONNECT_PAUSE = 0.1  # seconds

state = State()
_config_watcher = ConfigWatcher(CONFIG_PATH)
# Acquired (and never released) by the first lifespan of the process: it runs the per-process tasks.
_process_tasks = threading.Lock()


async def run_every(seconds: float, task: Callable[..., Awaitable[None]], *args: object) -> None:
    """Await ``task(*args)`` every ``seconds``; a failure is printed and the loop goes on."""
    while True:
        try:
            await task(*args)
        except Exception:  # noqa: BLE001
            traceback.print_exc()
        await asyncio.sleep(seconds)


async def reload_config() -> None:
    """Pick up a new config.json."""
    _config_watcher.reload(state)


async def rotate_pools(pools: list[AsyncPool]) -> None:
    """Recreate every connection of the loop's pools."""
    for pool in pools:
        # one pool at a time, as in the sync variant (QuoteService.rotate_db_connections)
        await pool.rotate()  # noqa: WPS476 - hot path: the other pools keep serving requests


async def consume_stream() -> None:
    """Apply push-stream messages to ``state``; reconnect after a failure."""
    while True:
        try:
            await _read_stream()
        except Exception:  # noqa: BLE001
            traceback.print_exc()
            await asyncio.sleep(STREAM_RECONNECT_PAUSE)


async def _read_stream() -> None:
    reader, writer = await asyncio.open_connection(*STREAM_ADDR)
    state.stream_reconnects += 1
    with contextlib.closing(writer):
        while True:
            seq, sym, rate = STREAM_MSG.unpack_from(await reader.readexactly(STREAM_MSG_SIZE))
            state.rates = {**state.rates, sym: rate}  # publish a new snapshot, never mutate
            state.stream_seq = seq


def start_process_tasks() -> list[asyncio.Task[None]]:
    """First call in the process: start the tasks that update ``state``; later calls start nothing."""
    if not _process_tasks.acquire(blocking=False):
        return []
    return [asyncio.create_task(run_every(CONFIG_PERIOD, reload_config)), asyncio.create_task(consume_stream())]
