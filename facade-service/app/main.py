from __future__ import annotations

import asyncio
import os
import random
import uuid
from typing import List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

app = FastAPI(title="facade-service", version="2.0.0")

# Comma-separated list of logging-service URLs
_raw = os.getenv(
    "LOGGING_URLS",
    "http://logging-service-1:8001,http://logging-service-2:8001,http://logging-service-3:8001"
)
LOGGING_URLS: List[str] = [u.strip() for u in _raw.split(",")]

COUNTER_URL = os.getenv("COUNTER_URL", "http://counter-service:8002")
TIMEOUT_SEC = float(os.getenv("TIMEOUT_SEC", "2.0"))
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", "0.3"))


class ClientPostIn(BaseModel):
    msg: str = Field(..., description="Message from client")


class ClientPostOut(BaseModel):
    id: str
    msg: str
    logging_status: str
    used_url: str


@app.get("/health", tags=["system"])
def health():
    return {"status": "ok", "logging_urls": LOGGING_URLS, "counter_url": COUNTER_URL}


async def post_to_logging(msg_id: str, msg: str) -> Tuple[bool, str, Optional[str]]:
    """Try each logging-service instance in random order until one succeeds."""
    urls = LOGGING_URLS.copy()
    random.shuffle(urls)

    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT_SEC)) as client:
        for url in urls:
            try:
                resp = await client.post(f"{url}/log", json={"id": msg_id, "msg": msg})
                resp.raise_for_status()
                print(f"[POST] Delivered to {url} id={msg_id}")
                return True, url, None
            except Exception as e:
                print(f"[POST] {url} failed: {e}, trying next...")

    return False, "", "All logging-service instances unavailable"


async def get_from_logging() -> Tuple[bool, str, Optional[str]]:
    """Try each logging-service instance in random order until one succeeds."""
    urls = LOGGING_URLS.copy()
    random.shuffle(urls)

    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT_SEC)) as client:
        for url in urls:
            try:
                resp = await client.get(f"{url}/logs")
                resp.raise_for_status()
                print(f"[GET] Read from {url}")
                return True, resp.text, None
            except Exception as e:
                print(f"[GET] {url} failed: {e}, trying next...")

    return False, "", "All logging-service instances unavailable"


@app.post("/message", response_model=ClientPostOut, tags=["client"])
async def client_post_message(payload: ClientPostIn):
    msg_id = str(uuid.uuid4())
    ok, used_url, err = await post_to_logging(msg_id, payload.msg)
    if not ok:
        raise HTTPException(status_code=502, detail=err)
    return ClientPostOut(id=msg_id, msg=payload.msg, logging_status="delivered", used_url=used_url)


@app.get("/messages", tags=["client"], response_class=PlainTextResponse)
async def client_get_messages():
    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT_SEC)) as client:
        try:
            cs = await client.get(f"{COUNTER_URL}/message")
            cs.raise_for_status()
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"counter-service unavailable: {e}")

    ok, logs_text, err = await get_from_logging()
    if not ok:
        raise HTTPException(status_code=502, detail=err)

    return f"{cs.text}\n{logs_text}"