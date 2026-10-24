"""Print result tables from results/*.jsonl (one JSON line per run, plus optional .rss sidecar).

    python print_results.py results/docker_results_heavy.jsonl [more files...]

Several files are merged; for each label the best run (max rps) is shown with the spread of all runs.
"""

import collections
import json
import os
import sys
from collections.abc import Iterator
from typing import Any

LABEL = "label"
RPS = "rps"
ROW = "{label:42} {n:>2} {rps:>6} {all_rps:>24} {p50:>7} {p99:>8} {mem:>9} {cpu:>8} {cpu_ms:>10}\n"
HEADER = ROW.format(
    label=LABEL,
    n="n",
    rps=RPS,
    all_rps="all rps",
    p50="p50",
    p99="p99",
    mem="mem",
    cpu="cpu",
    cpu_ms="cpu ms/req",
)
DEFAULT_PATHS = ("results/docker_results_heavy.jsonl",)
PERCENT = 100
MS_PER_S = 1000
UNKNOWN = "?"

Run = dict[str, Any]
Usage = tuple[Any, Any]  # memory and CPU of the server container, from the .rss sidecar


class Table:
    """Runs grouped by label, each with the container usage measured during it."""

    def __init__(self) -> None:
        """Start empty."""
        self.runs: dict[str, list[Run]] = collections.defaultdict(list)
        self.usage: dict[str, list[Usage]] = collections.defaultdict(list)

    def add_file(self, path: str) -> None:
        """Add the runs of ``path``; the n-th sidecar line of a label belongs to its n-th run."""
        sidecar = _read_sidecar(f"{path}.rss")
        for run in _read_jsonl(path):
            label = run[LABEL]
            self.runs[label].append(run)
            metas = sidecar[label]
            meta = metas.pop(0) if metas else {}
            self.usage[label].append(_usage(meta))

    def rows(self) -> Iterator[str]:
        """Header, then one line per label: its best run and the rps of all runs."""
        yield HEADER
        for label, runs in self.runs.items():
            yield _row(label, runs, self.usage[label])


def _read_jsonl(path: str) -> list[Run]:
    with open(path) as lines:
        return [json.loads(line) for line in lines if line.strip()]


def _read_sidecar(path: str) -> dict[str, list[Run]]:
    by_label: dict[str, list[Run]] = collections.defaultdict(list)
    if os.path.exists(path):
        for meta in _read_jsonl(path):
            by_label[meta[LABEL]].append(meta)
    return by_label


def _usage(meta: Run) -> Usage:
    mem = meta.get("server_mem", meta.get("server_rss_mb", UNKNOWN))
    return mem, meta.get("server_cpu", UNKNOWN)


def _row(label: str, runs: list[Run], usage: list[Usage]) -> str:
    all_rps = [run[RPS] for run in runs]
    best_index = all_rps.index(max(all_rps))  # the first run with the max rps
    best = runs[best_index]
    mem, cpu = usage[best_index]
    return ROW.format(
        label=label,
        n=len(runs),
        rps=best[RPS],
        all_rps=str(all_rps),
        p50=best["p50_ms"],
        p99=best["p99_ms"],
        mem=f"{mem}MB" if isinstance(mem, (int, float)) else str(mem),
        cpu=str(cpu),
        cpu_ms=_cpu_ms_per_request(cpu, best[RPS]),
    )


def _cpu_ms_per_request(cpu: Any, rps: int) -> str:
    """CPU time per request from docker stats' "CPU %" ("?" when unknown)."""
    try:
        percent = float(str(cpu).rstrip("%"))
    except ValueError:
        return UNKNOWN
    if not rps:
        return UNKNOWN
    cpu_ms = percent / PERCENT * MS_PER_S / rps
    return f"{cpu_ms:.1f}"


def main(paths: list[str]) -> None:
    """Print the table for the merged result files ``paths``."""
    table = Table()
    for path in paths:
        table.add_file(path)
    sys.stdout.writelines(table.rows())


if __name__ == "__main__":
    main(sys.argv[1:] or list(DEFAULT_PATHS))
