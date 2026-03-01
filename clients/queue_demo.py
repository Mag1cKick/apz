from __future__ import annotations

import argparse
import multiprocessing as mp
import time

from common import get_client, shutdown_safely

QUEUE_NAME = "bounded-queue"
STOP = "__STOP__"


def reset_queue() -> None:
    client = get_client("queue-reset")
    try:
        q = client.get_queue(QUEUE_NAME).blocking()
        drained = 0
        while True:
            item = q.poll(timeout=0.1)
            if item is None:
                break
            drained += 1
        print(f"[RESET] drained={drained}")
    finally:
        shutdown_safely(client)


def producer(total: int, delay: float) -> None:
    client = get_client("queue-producer")
    try:
        q = client.get_queue(QUEUE_NAME).blocking()
        for i in range(1, total + 1):
            q.put(i)
            print(f"[PRODUCER] put {i}")
            if delay > 0:
                time.sleep(delay)
        q.put(STOP)
        q.put(STOP)
        print("[PRODUCER] sent STOP x2")
    finally:
        shutdown_safely(client)


def consumer(name: str, result_queue) -> None:
    client = get_client(f"queue-{name}")
    consumed = []
    try:
        q = client.get_queue(QUEUE_NAME).blocking()
        while True:
            item = q.take()
            if item == STOP:
                print(f"[{name}] STOP")
                break
            consumed.append(item)
            print(f"[{name}] got {item}")
        result_queue.put((name, consumed))
    finally:
        shutdown_safely(client)


def run_demo(total: int, producer_delay: float) -> None:
    reset_queue()

    ctx = mp.get_context("spawn")
    rq = ctx.Queue()

    c1 = ctx.Process(target=consumer, args=("C1", rq))
    c2 = ctx.Process(target=consumer, args=("C2", rq))
    p = ctx.Process(target=producer, args=(total, producer_delay))

    c1.start()
    c2.start()
    time.sleep(0.5)
    p.start()

    p.join()
    res1 = rq.get()
    res2 = rq.get()
    c1.join()
    c2.join()

    results = dict([res1, res2])
    all_items = sorted(results["C1"] + results["C2"])
    print("\n=== QUEUE DEMO SUMMARY ===")
    print(f"C1_count={len(results['C1'])}")
    print(f"C2_count={len(results['C2'])}")
    print(f"Total_consumed={len(results['C1']) + len(results['C2'])}")
    print(f"First_10_C1={results['C1'][:10]}")
    print(f"First_10_C2={results['C2'][:10]}")
    print(f"All_items_1_to_{total}_received={all_items == list(range(1, total+1))}")

    client = get_client("queue-check")
    try:
        q = client.get_queue(QUEUE_NAME).blocking()
        print(f"Queue_size_after={q.size()}")
    finally:
        shutdown_safely(client)


def fill_no_consumer(timeout_sec: float) -> None:
    reset_queue()
    client = get_client("queue-fill")
    try:
        q = client.get_queue(QUEUE_NAME).blocking()
        for i in range(1, 11):
            q.put(i)
        print(f"[FILL] queue_size={q.size()} remaining_capacity={q.remaining_capacity()}")
        t0 = time.perf_counter()
        ok = q.offer(999, timeout=timeout_sec)
        dt = time.perf_counter() - t0
        print(f"[FULL] offer(timeout={timeout_sec}) -> {ok} in {dt:.3f}s")
    finally:
        shutdown_safely(client)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["run-demo", "fill-no-consumer", "reset"])
    parser.add_argument("--total", type=int, default=100)
    parser.add_argument("--producer-delay", type=float, default=0.0)
    parser.add_argument("--timeout", type=float, default=3.0)
    args = parser.parse_args()

    if args.action == "run-demo":
        run_demo(total=args.total, producer_delay=args.producer_delay)
    elif args.action == "fill-no-consumer":
        fill_no_consumer(timeout_sec=args.timeout)
    else:
        reset_queue()


if __name__ == "__main__":
    mp.freeze_support()
    main()
