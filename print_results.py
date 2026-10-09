"""Print result tables from results/*.jsonl (one JSON line per run, plus optional .rss sidecar).

    python print_results.py results/docker_results_heavy.jsonl [more files...]

Several files are merged; for each label the best run (max rps) is shown with the spread of all runs.
"""

import collections
import json
import os
import sys


def main(paths: list[str]) -> None:
    runs: dict[str, list[dict]] = collections.defaultdict(list)
    cpu: dict[str, list[tuple[str, str]]] = collections.defaultdict(list)
    for path in paths:
        rows = [json.loads(line) for line in open(path) if line.strip()]
        side = path + ".rss"
        meta = [json.loads(line) for line in open(side) if line.strip()] if os.path.exists(side) else []
        by_label = collections.defaultdict(list)
        for m in meta:
            by_label[m["label"]].append(m)
        for r in rows:
            runs[r["label"]].append(r)
            m = by_label[r["label"]].pop(0) if by_label[r["label"]] else {}
            cpu[r["label"]].append((m.get("server_mem", m.get("server_rss_mb", "?")), m.get("server_cpu", "?")))
    print(f"{'label':42} {'n':>2} {'rps':>6} {'all rps':>24} {'p50':>7} {'p99':>8} {'mem':>9} {'cpu':>8} {'cpu ms/req':>10}")
    for label, rs in runs.items():
        i = max(range(len(rs)), key=lambda k: rs[k]["rps"])
        b = rs[i]
        mem, c = cpu[label][i]
        try:
            cpu_ms = f"{float(str(c).rstrip('%')) / 100 * 1000 / b['rps']:.1f}"
        except (ValueError, ZeroDivisionError):
            cpu_ms = "?"
        mem_s = f"{mem}MB" if isinstance(mem, (int, float)) else str(mem)
        print(f"{label:42} {len(rs):>2} {b['rps']:>6} {str([r['rps'] for r in rs]):>24} {b['p50_ms']:>7} {b['p99_ms']:>8} {mem_s:>9} {str(c):>8} {cpu_ms:>10}")


if __name__ == "__main__":
    main(sys.argv[1:] or ["results/docker_results_heavy.jsonl"])
