from __future__ import annotations

import asyncio
import json
import os
import random
import time
import uuid
from typing import List, Optional, Tuple

import consul
import hazelcast
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

app = FastAPI(title="facade-service", version="4.0.0")

CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", "8500"))
SERVICE_ID = os.getenv("SERVICE_ID", "facade-service-1")
SERVICE_NAME = "facade-service"
SERVICE_ADDRESS = os.getenv("SERVICE_ADDRESS", "facade-service")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8000"))
TIMEOUT_SEC = float(os.getenv("TIMEOUT_SEC", "2.0"))

consul_client: Optional[consul.Consul] = None
hz_client: Optional[hazelcast.HazelcastClient] = None
counter_queue = None
_queue_name: str = "counter-queue"


class ClientPostIn(BaseModel):
    msg: str = Field(..., description="Message from client")


class ClientPostOut(BaseModel):
    id: str
    msg: str
    logging_status: str
    used_url: str


def _kv_get(key: str, default: str = "") -> str:
    for attempt in range(10):
        try:
            _, data = consul_client.kv.get(key)
            if data and data.get("Value"):
                return data["Value"].decode()
        except Exception as e:
            print(f"[CONSUL] KV '{key}' attempt {attempt + 1}: {e}")
        time.sleep(2)
    print(f"[CONSUL] KV '{key}' not found, using default: '{default}'")
    return default


def _discover(service_name: str) -> List[str]:
    try:
        _, services = consul_client.health.service(service_name, passing=True)
        urls = [
            f"http://{s['Service']['Address']}:{s['Service']['Port']}"
            for s in services
        ]
        return urls
    except Exception as e:
        print(f"[CONSUL] Discovery '{service_name}' error: {e}")
        return []


@app.on_event("startup")
async def startup():
    global consul_client, hz_client, counter_queue, _queue_name

    consul_client = consul.Consul(host=CONSUL_HOST, port=CONSUL_PORT)

    hz_members_str = _kv_get(
        "hazelcast/cluster-members",
        "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701",
    )
    _queue_name = _kv_get("mq/queue-name", "counter-queue")
    print(f"[CONSUL] HZ members: {hz_members_str}  queue: {_queue_name}")

    members = hz_members_str.split(",")
    hz_client = hazelcast.HazelcastClient(
        cluster_members=members,
        cluster_name="dev",
        reconnect_mode=hazelcast.config.ReconnectMode.ASYNC,
        connection_timeout=5.0,
        retry_initial_backoff=1.0,
        retry_max_backoff=10.0,
        retry_multiplier=1.5,
        cluster_connect_timeout=30.0,
    )
    counter_queue = hz_client.get_queue(_queue_name).blocking()
    print(f"[HZ] Connected; queue '{_queue_name}' ready")

    try:
        consul_client.agent.service.register(
            name=SERVICE_NAME,
            service_id=SERVICE_ID,
            address=SERVICE_ADDRESS,
            port=SERVICE_PORT,
            check=consul.Check.http(
                f"http://{SERVICE_ADDRESS}:{SERVICE_PORT}/health",
                interval="10s",
                timeout="5s",
                deregister="30s",
            ),
        )
        print(f"[CONSUL] Registered {SERVICE_ID} @ {SERVICE_ADDRESS}:{SERVICE_PORT}")
    except Exception as e:
        print(f"[CONSUL] Registration failed: {e}")


@app.on_event("shutdown")
def shutdown():
    try:
        consul_client.agent.service.deregister(SERVICE_ID)
        print(f"[CONSUL] Deregistered {SERVICE_ID}")
    except Exception:
        pass
    if hz_client:
        hz_client.shutdown()


async def _get_service_urls(name: str) -> List[str]:
    loop = asyncio.get_event_loop()
    for attempt in range(5):
        urls = await loop.run_in_executor(None, _discover, name)
        if urls:
            return urls
        print(f"[CONSUL] No healthy '{name}' instances, retry {attempt + 1}")
        await asyncio.sleep(1)
    return []


async def _post_to_logging(
    msg_id: str, msg: str, urls: List[str]
) -> Tuple[bool, str, Optional[str]]:
    shuffled = urls.copy()
    random.shuffle(shuffled)
    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT_SEC)) as client:
        for url in shuffled:
            try:
                resp = await client.post(f"{url}/log", json={"id": msg_id, "msg": msg})
                resp.raise_for_status()
                print(f"[POST] Delivered to {url} id={msg_id}")
                return True, url, None
            except Exception as e:
                print(f"[POST] {url} failed: {e}")
    return False, "", "All logging-service instances unavailable"


async def _get_from_logging(urls: List[str]) -> Tuple[bool, str, Optional[str]]:
    shuffled = urls.copy()
    random.shuffle(shuffled)
    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT_SEC)) as client:
        for url in shuffled:
            try:
                resp = await client.get(f"{url}/logs")
                resp.raise_for_status()
                print(f"[GET] Logs from {url}")
                return True, resp.text, None
            except Exception as e:
                print(f"[GET] {url} failed: {e}")
    return False, "", "All logging-service instances unavailable"


@app.get("/health", tags=["system"])
def health():
    return {"status": "ok"}


@app.post("/message", response_model=ClientPostOut, tags=["client"])
async def client_post_message(payload: ClientPostIn):
    msg_id = str(uuid.uuid4())

    item = json.dumps({"id": msg_id, "msg": payload.msg})
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, counter_queue.offer, item)
    print(f"[MQ] Enqueued id={msg_id} msg='{payload.msg}'")

    logging_urls = await _get_service_urls("logging-service")
    if not logging_urls:
        raise HTTPException(status_code=503, detail="No logging-service instances available")
    ok, used_url, err = await _post_to_logging(msg_id, payload.msg, logging_urls)
    if not ok:
        raise HTTPException(status_code=502, detail=err)

    return ClientPostOut(id=msg_id, msg=payload.msg, logging_status="delivered", used_url=used_url)


@app.get("/messages", tags=["client"], response_class=PlainTextResponse)
async def client_get_messages():
    counter_text = "null"
    counter_urls = await _get_service_urls("counter-service")
    if counter_urls:
        counter_url = random.choice(counter_urls)
        async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT_SEC)) as client:
            try:
                cs = await client.get(f"{counter_url}/message")
                cs.raise_for_status()
                counter_text = cs.text
            except Exception as e:
                print(f"[GET] counter-service unavailable: {e}")
    else:
        print("[GET] No counter-service instances in Consul")

    logging_urls = await _get_service_urls("logging-service")
    if not logging_urls:
        raise HTTPException(status_code=503, detail="No logging-service instances available")
    ok, logs_text, err = await _get_from_logging(logging_urls)
    if not ok:
        raise HTTPException(status_code=502, detail=err)

    return f"Counter: {counter_text}\nLogs: {logs_text}"
