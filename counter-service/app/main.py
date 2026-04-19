from __future__ import annotations

import json
import os
import threading
import time

import hazelcast
import httpx
import mysql.connector
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI(title="counter-service", version="3.0.0")

DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "root")
DB_NAME = os.getenv("DB_NAME", "counterdb")

HZ_CLUSTER_MEMBERS = os.getenv(
    "HZ_CLUSTER_MEMBERS",
    "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701",
)
QUEUE_NAME = "counter-queue"
CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8010")
SELF_URL = os.getenv("SELF_URL", "http://counter-service:8002")

hz_client = None
counter_queue = None
_running = True


def get_conn():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS,
        database=DB_NAME,
    )


def _consume_loop():
    global _running
    print("[MQ] Consumer thread started")
    while _running:
        try:
            item = counter_queue.poll(timeout=1)
            if item is None:
                continue
            data = json.loads(item)
            msg_id, msg = data["id"], data["msg"]
            conn = get_conn()
            cur = conn.cursor()
            cur.execute(
                "INSERT IGNORE INTO messages (id, content) VALUES (%s, %s)",
                (msg_id, msg),
            )
            conn.commit()
            cur.close()
            conn.close()
            print(f"[MQ] Stored: id={msg_id} msg='{msg}'")
        except Exception as e:
            print(f"[MQ] Error in consumer: {e}")
            time.sleep(1)
    print("[MQ] Consumer thread stopped")


@app.on_event("startup")
def startup():
    global hz_client, counter_queue

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            auto_id INT AUTO_INCREMENT PRIMARY KEY,
            id      VARCHAR(36) UNIQUE NOT NULL,
            content VARCHAR(255) NOT NULL
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("[DB] MySQL connected and table ready")

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
    print(f"[HZ] Connected; consuming from queue '{QUEUE_NAME}'")

    for attempt in range(10):
        try:
            with httpx.Client(timeout=5.0) as client:
                client.post(
                    f"{CONFIG_SERVER_URL}/register",
                    json={"name": "counter-service", "url": SELF_URL},
                )
            print(f"[CONFIG] Registered counter-service -> {SELF_URL}")
            break
        except Exception as e:
            print(f"[CONFIG] Registration attempt {attempt + 1} failed: {e}")
            time.sleep(2)

    t = threading.Thread(target=_consume_loop, daemon=True)
    t.start()


@app.on_event("shutdown")
def shutdown():
    global _running
    _running = False
    if hz_client:
        hz_client.shutdown()


@app.get("/health", tags=["system"])
def health():
    return {"status": "ok"}


@app.get("/message", tags=["message"], response_class=PlainTextResponse)
def get_message():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT content FROM messages ORDER BY auto_id")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        return "No messages processed yet"
    msgs = ", ".join(r[0] for r in rows)
    return f"Processed {len(rows)} message(s): {msgs}"
