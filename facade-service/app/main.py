from __future__ import annotations

import asyncio
import os
import uuid
from typing import Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field

app = FastAPI(title="facade-service", version="1.0.0")

LOGGING_URL = os.getenv("LOGGING_URL", "http://logging-service:8001")
MESSAGES_URL = os.getenv("MESSAGES_URL", "http://messages-service:8002")

RETRY_COUNT = int(os.getenv("RETRY_COUNT", "5"))
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", "0.3"))
TIMEOUT_SEC = float(os.getenv("TIMEOUT_SEC", "1.0"))


class ClientPostIn(BaseModel):
    msg: str = Field(..., description="Message from client")


class ClientPostOut(BaseModel):
    id: str
    msg: str
    logging_status: str
    attempts: int


@app.get("/health", tags=["system"])
def health():
    return {
        "status": "ok",
        "logging_url": LOGGING_URL,
        "messages_url": MESSAGES_URL,
        "retry_count": RETRY_COUNT,
        "timeout_sec": TIMEOUT_SEC,
    }


async def post_with_retry(
    url: str,
    json_payload: dict,
    *,
    attempts: int,
    base_delay: float,
    timeout_sec: float,
    fail_first_n: int = 0,
) -> Tuple[bool, int, Optional[str]]:
    last_error: Optional[str] = None

    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout_sec)) as client:
        for i in range(1, attempts + 1):
            if fail_first_n > 0:
                fail_first_n -= 1
                last_error = "forced failure (test hook)"
                print(f"[RETRY] attempt={i}/{attempts} -> {last_error}")
            else:
                try:
                    resp = await client.post(url, json=json_payload)
                    resp.raise_for_status()
                    return True, i, None
                except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
                    last_error = f"{type(e).__name__}: {e}"
                    print(f"[RETRY] attempt={i}/{attempts} -> {last_error}")
                except httpx.HTTPStatusError as e:
                    last_error = f"HTTPStatusError: {e.response.status_code} {e.response.text}"
                    return False, i, last_error

            if i < attempts:
                delay = base_delay * (2 ** (i - 1))
                await asyncio.sleep(delay)

    return False, attempts, last_error


@app.post("/message", response_model=ClientPostOut, tags=["client"])
async def client_post_message(
    payload: ClientPostIn,
    fail_first_n: int = Query(0, ge=0, le=10, description="Test hook: force first N attempts to fail before real request"),
):
    msg_id = str(uuid.uuid4())

    ok, used, err = await post_with_retry(
        f"{LOGGING_URL}/log",
        {"id": msg_id, "msg": payload.msg},
        attempts=RETRY_COUNT,
        base_delay=RETRY_BASE_DELAY,
        timeout_sec=TIMEOUT_SEC,
        fail_first_n=fail_first_n,
    )

    if not ok:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "Failed to deliver message to logging-service",
                "attempts": used,
                "last_error": err,
            },
        )

    return ClientPostOut(
        id=msg_id,
        msg=payload.msg,
        logging_status="delivered",
        attempts=used,
    )


@app.get("/messages", tags=["client"], response_class=PlainTextResponse)
async def client_get_messages():
    async with httpx.AsyncClient(timeout=httpx.Timeout(TIMEOUT_SEC)) as client:
        try:
            ms = await client.get(f"{MESSAGES_URL}/message")
            ms.raise_for_status()
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"messages-service unavailable: {e}")

        try:
            lg = await client.get(f"{LOGGING_URL}/logs")
            lg.raise_for_status()
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"logging-service unavailable: {e}")

    return f"{ms.text}\n{lg.text}"
