from __future__ import annotations

import argparse
from typing import List

from common import get_client, shutdown_safely


def cmd_fill(map_name: str, start: int, end: int, value_prefix: str) -> None:
    client = get_client("task3-fill")
    try:
        m = client.get_map(map_name).blocking()
        for i in range(start, end + 1):
            m.put(i, f"{value_prefix}{i}")
        print(f"[FILL] map={map_name} inserted={end-start+1} keys={start}..{end}")
        print(f"[FILL] current_size={m.size()}")
    finally:
        shutdown_safely(client)


def cmd_verify(map_name: str, start: int, end: int) -> None:
    client = get_client("task3-verify")
    try:
        m = client.get_map(map_name).blocking()
        missing: List[int] = []
        mismatched: List[int] = []
        for i in range(start, end + 1):
            v = m.get(i)
            if v is None:
                missing.append(i)
            elif not str(v).endswith(str(i)):
                mismatched.append(i)
        print(f"[VERIFY] map={map_name} expected_keys={start}..{end}")
        print(f"[VERIFY] size={m.size()}")
        print(f"[VERIFY] missing_count={len(missing)}")
        if missing:
            print(f"[VERIFY] missing_sample={missing[:20]}")
        print(f"[VERIFY] mismatched_count={len(mismatched)}")
        if mismatched:
            print(f"[VERIFY] mismatched_sample={mismatched[:20]}")
    finally:
        shutdown_safely(client)


def cmd_clear(map_name: str) -> None:
    client = get_client("task3-clear")
    try:
        m = client.get_map(map_name).blocking()
        m.clear()
        print(f"[CLEAR] map={map_name} size_after={m.size()}")
    finally:
        shutdown_safely(client)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["fill", "verify", "clear"])
    parser.add_argument("--map", dest="map_name", default="demo-map")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--end", type=int, default=1000)
    parser.add_argument("--value-prefix", default="value-")
    args = parser.parse_args()

    if args.action == "fill":
        cmd_fill(args.map_name, args.start, args.end, args.value_prefix)
    elif args.action == "verify":
        cmd_verify(args.map_name, args.start, args.end)
    else:
        cmd_clear(args.map_name)


if __name__ == "__main__":
    main()
