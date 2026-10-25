"""PROTOTYPE. Result line of loadgen.py: rps, latency percentiles, errors and state freshness."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

MS_PER_S = 1000
PERCENTILES = (("p50_ms", 0.5), ("p95_ms", 0.95), ("p99_ms", 0.99))
FRESHNESS_KEYS = ("cfg_version", "stream_seq", "pool_gen")

Sample = dict[str, Any]
Summary = dict[str, Any]


class WorkerStats(Protocol):
    """What one load-generating connection collected."""

    latencies: list[float]
    samples: list[Sample]  # sampled response bodies
    errors: int


Workers = Sequence[WorkerStats]


def summarize(label: str, conc: int, workers: Workers, elapsed: float) -> Summary:
    """Result line: rps, latency percentiles, errors and the observed range of every freshness key."""
    latencies = [latency for stats in workers for latency in stats.latencies]
    latencies.sort()
    rps = round(len(latencies) / elapsed)
    summary: Summary = {"label": label, "conc": conc, "rps": rps}
    summary.update(_latency_ms(latencies))
    summary["errors"] = sum(stats.errors for stats in workers)
    summary.update(_freshness(workers))
    return summary


def _latency_ms(latencies: list[float]) -> Summary:
    """p50/p95/p99 and max of the sorted ``latencies`` in ms (-1 when there are none)."""
    stats: Summary = {name: _percentile(latencies, quantile) for name, quantile in PERCENTILES}
    if latencies:
        stats["max_ms"] = round(latencies[-1] * MS_PER_S, 1)
    else:
        stats["max_ms"] = -1
    return stats


def _percentile(latencies: list[float], quantile: float) -> float:
    if not latencies:
        return -1
    count = len(latencies)
    index = min(count - 1, int(quantile * count))
    return round(latencies[index] * MS_PER_S, 2)


def _freshness(workers: Workers) -> Summary:
    """[min, max] of every FRESHNESS_KEYS value in the sampled bodies (None without samples)."""
    samples = [sample for stats in workers for sample in stats.samples]
    return {key: _value_range(samples, key) for key in FRESHNESS_KEYS}


def _value_range(samples: list[Sample], key: str) -> list[int] | None:
    if not samples:
        return None
    observed = [sample.get(key, 0) for sample in samples]
    return [min(observed), max(observed)]
