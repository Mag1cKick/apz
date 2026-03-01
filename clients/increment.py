from __future__ import annotations

import argparse
import multiprocessing as mp
import time
from dataclasses import dataclass, asdict

from common import get_client, shutdown_safely


@dataclass
class WorkerResult:
    worker: str
    mode: str
    iterations: int
    seconds: float
    optimistic_retries: int = 0
    errors: int = 0


def _increment_no_lock(m, key: str, iterations: int) -> tuple[int, int]:
    errors = 0
    for _ in range(iterations):
        v = m.get(key)
        if v is None:
            v = 0
        try:
            m.put(key, int(v) + 1)
        except Exception:
            errors += 1
    return 0, errors


def _increment_pessimistic(m, key: str, iterations: int) -> tuple[int, int]:
    errors = 0
    for _ in range(iterations):
        try:
            m.lock(key)
            v = m.get(key)
            if v is None:
                v = 0
            m.put(key, int(v) + 1)
        except Exception:
            errors += 1
        finally:
            try:
                m.unlock(key)
            except Exception:
                pass
    return 0, errors


def _increment_optimistic(m, key: str, iterations: int) -> tuple[int, int]:
    retries = 0
    errors = 0
    for _ in range(iterations):
        while True:
            try:
                old = m.get(key)
                if old is None:
                    prev = m.put_if_absent(key, 0)
                    old = 0 if prev is None else prev
                new = int(old) + 1
                if m.replace_if_same(key, old, new):
                    break
                retries += 1
            except Exception:
                errors += 1
                retries += 1
    return retries, errors


def worker_process(mode: str, map_name: str, key: str, iterations: int, start_evt, result_queue, idx: int) -> None:
    client = get_client(f"inc-{mode}-{idx}")
    t0 = t1 = None
    try:
        m = client.get_map(map_name).blocking()
        m.put_if_absent(key, 0)
        start_evt.wait()
        t0 = time.perf_counter()
        if mode == "no_lock":
            retries, errors = _increment_no_lock(m, key, iterations)
        elif mode == "pessimistic":
            retries, errors = _increment_pessimistic(m, key, iterations)
        elif mode == "optimistic":
            retries, errors = _increment_optimistic(m, key, iterations)
        else:
            raise ValueError(f"Unknown mode: {mode}")
        t1 = time.perf_counter()
        result_queue.put(WorkerResult(
            worker=f"W{idx}",
            mode=mode,
            iterations=iterations,
            seconds=t1 - t0,
            optimistic_retries=retries,
            errors=errors,
        ))
    except Exception:
        if t0 is None:
            t0 = time.perf_counter()
        t1 = time.perf_counter()
        result_queue.put(WorkerResult(
            worker=f"W{idx}",
            mode=mode,
            iterations=iterations,
            seconds=t1 - t0,
            optimistic_retries=0,
            errors=1,
        ))
        raise
    finally:
        shutdown_safely(client)


def run_experiment(mode: str, map_name: str, key: str, iterations: int, workers: int) -> None:
    ctl = get_client("inc-control")
    try:
        m = ctl.get_map(map_name).blocking()
        m.put(key, 0)
        print(f"[SETUP] map={map_name} key={key} initial={m.get(key)} mode={mode}")

        ctx = mp.get_context("spawn")
        start_evt = ctx.Event()
        result_queue = ctx.Queue()
        procs = []
        for i in range(workers):
            p = ctx.Process(
                target=worker_process,
                args=(mode, map_name, key, iterations, start_evt, result_queue, i + 1),
            )
            p.start()
            procs.append(p)

        time.sleep(1.0)
        wall_t0 = time.perf_counter()
        start_evt.set()

        results = []
        for _ in procs:
            results.append(result_queue.get())

        for p in procs:
            p.join()

        wall_t1 = time.perf_counter()
        final_value = m.get(key)
        expected = workers * iterations
        print("\n=== RESULTS ===")
        for r in sorted(results, key=lambda x: x.worker):
            print(asdict(r))
        print(f"[SUMMARY] mode={mode}")
        print(f"[SUMMARY] expected={expected}")
        print(f"[SUMMARY] actual={final_value}")
        print(f"[SUMMARY] lost_updates={expected - int(final_value)}")
        print(f"[SUMMARY] wall_seconds={wall_t1 - wall_t0:.4f}")
    finally:
        shutdown_safely(ctl)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["no_lock", "pessimistic", "optimistic"], required=True)
    parser.add_argument("--map", dest="map_name", default="counter-map")
    parser.add_argument("--key", default="key")
    parser.add_argument("--iterations", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()

    run_experiment(args.mode, args.map_name, args.key, args.iterations, args.workers)


if __name__ == "__main__":
    mp.freeze_support()
    main()
