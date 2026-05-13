from __future__ import annotations

import json
import os
import threading
import time

import consul
import hazelcast
import mysql.connector
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI(title="counter-service", version="4.0.0")

DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "root")
DB_NAME = os.getenv("DB_NAME", "counterdb")

CONSUL_HOST = os.getenv("CONSUL_HOST", "consul")
CONSUL_PORT = int(os.getenv("CONSUL_PORT", "8500"))
SERVICE_ID = os.getenv("SERVICE_ID", "counter-service-1")
SERVICE_NAME = "counter-service"
SERVICE_ADDRESS = os.getenv("SERVICE_ADDRESS", "counter-service")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8002"))

consul_client: consul.Consul = None
hz_client = None
counter_queue = None
_running = True


def get_conn():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS,
        database=DB_NAME,
    )


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
            print(f"[MQ] Error: {e}")
            time.sleep(1)
    print("[MQ] Consumer thread stopped")


@app.on_event("startup")
def startup():
    global consul_client, hz_client, counter_queue

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

    consul_client = consul.Consul(host=CONSUL_HOST, port=CONSUL_PORT)

    hz_members_str = _kv_get(
        "hazelcast/cluster-members",
        "hazelcast-1:5701,hazelcast-2:5701,hazelcast-3:5701",
    )
    queue_name = _kv_get("mq/queue-name", "counter-queue")
    print(f"[CONSUL] HZ members: {hz_members_str}  queue: {queue_name}")

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
    counter_queue = hz_client.get_queue(queue_name).blocking()
    print(f"[HZ] Connected; consuming queue '{queue_name}'")

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

    t = threading.Thread(target=_consume_loop, daemon=True)
    t.start()


@app.on_event("shutdown")
def shutdown():
    global _running
    _running = False
    try:
        consul_client.agent.service.deregister(SERVICE_ID)
        print(f"[CONSUL] Deregistered {SERVICE_ID}")
    except Exception:
        pass
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
