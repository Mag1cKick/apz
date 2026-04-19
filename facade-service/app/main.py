from __future__ import annotations

import asyncio
import json
import os
import random
import uuid
from typing import List, Optional, Tuple

import hazelcast
import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

app = FastAPI(title="facade-service", version="3.0.0")

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8010")
SELF_URL = os.getenv("SELF_URL", "http://facade-service:8000")
HZ_CLUSTER_MEMBERS = os.getenv(
    "HZ_CLUSTER_MEMBERS",
    "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701",
)
QUEUE_NAME = "counter-queue"
TIMEOUT_SEC = float(os.getenv("TIMEOUT_SEC", "2.0"))

hz_client: Optional[hazelcast.HazelcastClient] = None
counter_queue = None


class ClientPostIn(BaseModel):
    msg: str = Field(..., description="Message from client")


class ClientPostOut(BaseModel):
    id: str
    msg: str
    logging_status: str
    used_url: str


@app.on_event("startup")
async def startup():
    global hz_client, counter_queue
    members = HZ_CLUSTER_MEMBERS.split(",")
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
    counter_queue = hz_client.get_queue(QUEUE_NAME).blocking()
    print(f"[HZ] Connected; queue '{QUEUE_NAME}' ready")

    async with httpx.AsyncClient(timeout=httpx.Timeout(5.0)) as client:
        for attempt in range(10):
            try:
                await client.post(
                    f"{CONFIG_SERVER_URL}/register",
                    json={"name": "facade-service", "url": SELF_URL},
                )
                print(f"[CONFIG] Registered facade-service -> {SELF_URL}")
                break
            except Exception as e:
                print(f"[CONFIG] Registration attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(2)


@app.on_event("shutdown")
def shutdown():
    if hz_client:
        hz_client.shutdown()


async def _get_service_urls(name: str) -> List[str]:
    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT_SEC)) as client:
        for attempt in range(5):
            try:
                resp = await client.get(f"{CONFIG_SERVER_URL}/services/{name}")
                if resp.status_code == 200:
                    urls = resp.json()
                    if urls:
                        return urls
            except Exception as e:
                print(f"[CONFIG] Lookup '{name}' attempt {attempt + 1}: {e}")
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
                print(f"[GET] Read logs from {url}")
                return True, resp.text, None
            except Exception as e:
                print(f"[GET] {url} failed: {e}")
    return False, "", "All logging-service instances unavailable"


@app.get("/health", tags=["system"])
async def health():
    return {"status": "ok", "config_server": CONFIG_SERVER_URL}


@app.post("/message", response_model=ClientPostOut, tags=["client"])
async def client_post_message(payload: ClientPostIn):
    msg_id = str(uuid.uuid4())

    # Push to Hazelcast Queue asynchronously — counter-service will consume
    item = json.dumps({"id": msg_id, "msg": payload.msg})
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, counter_queue.offer, item)
    print(f"[MQ] Enqueued id={msg_id} msg='{payload.msg}'")

    # Forward to one logging-service instance
    logging_urls = await _get_service_urls("logging-service")
    if not logging_urls:
        raise HTTPException(status_code=503, detail="No logging-service instances registered")
    ok, used_url, err = await _post_to_logging(msg_id, payload.msg, logging_urls)
    if not ok:
        raise HTTPException(status_code=502, detail=err)

    return ClientPostOut(id=msg_id, msg=payload.msg, logging_status="delivered", used_url=used_url)


@app.get("/messages", tags=["client"], response_class=PlainTextResponse)
async def client_get_messages():
    # Read from counter-service via HTTP GET (unchanged path)
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
                counter_text = "null"
    else:
        print("[GET] No counter-service registered in config-server")

    # Read logs from logging-service
    logging_urls = await _get_service_urls("logging-service")
    if not logging_urls:
        raise HTTPException(status_code=503, detail="No logging-service instances registered")
    ok, logs_text, err = await _get_from_logging(logging_urls)
    if not ok:
        raise HTTPException(status_code=502, detail=err)

    return f"Counter: {counter_text}\nLogs: {logs_text}"
