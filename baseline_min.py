"""PROTOTYPE. min-of-3 timing for N threads / N forked processes doing the same CPU work each."""
import os, sys, threading, time
def cpu_work(iters):
    s = 0
    for i in range(iters):
        s += i * i
    return s
cpu_work(1000)
def run_threads(n):
    ths = [threading.Thread(target=cpu_work, args=(2_000_000,)) for _ in range(n)]
    t0 = time.perf_counter(); [t.start() for t in ths]; [t.join() for t in ths]; return time.perf_counter() - t0
def run_procs(n):
    t0 = time.perf_counter(); pids = []
    for _ in range(n):
        pid = os.fork()
        if pid == 0: cpu_work(2_000_000); os._exit(0)
        pids.append(pid)
    for p in pids: os.waitpid(p, 0)
    return time.perf_counter() - t0
build = 'ft' if not sys._is_gil_enabled() else 'gil'
for n in (1, 2, 4):
    print(f"{build} n={n} threads={min(run_threads(n) for _ in range(3)):.3f} procs={min(run_procs(n) for _ in range(3)):.3f}")
