import os
from typing import List

import hazelcast


def _parse_members() -> List[str]:
    raw = os.getenv("HZ_MEMBERS", "127.0.0.1:5701,127.0.0.1:5702,127.0.0.1:5703")
    return [m.strip() for m in raw.split(",") if m.strip()]


def get_client(client_name: str | None = None):
    kwargs = dict(
        cluster_name=os.getenv("HZ_CLUSTER_NAME", "apz-hz"),
        cluster_members=_parse_members(),
        smart_routing=False,
    )
    if client_name:
        try:
            return hazelcast.HazelcastClient(client_name=client_name, **kwargs)
        except TypeError:
            return hazelcast.HazelcastClient(**kwargs)
    return hazelcast.HazelcastClient(**kwargs)


def shutdown_safely(client) -> None:
    try:
        client.shutdown()
    except Exception:
        pass
