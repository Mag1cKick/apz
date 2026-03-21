from __future__ import annotations

import os

import mysql.connector
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI(title="counter-service", version="2.0.0")

DB_HOST = os.getenv("DB_HOST", "mysql")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "root")
DB_NAME = os.getenv("DB_NAME", "counterdb")

STATIC_TEXT = os.getenv("STATIC_MESSAGE", "Not implemented yet")


def get_conn():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS,
        database=DB_NAME
    )


@app.on_event("startup")
def startup():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INT AUTO_INCREMENT PRIMARY KEY,
            content VARCHAR(255) NOT NULL
        )
    """)
    # Seed a static message if table is empty
    cur.execute("SELECT COUNT(*) FROM messages")
    (count,) = cur.fetchone()
    if count == 0:
        cur.execute("INSERT INTO messages (content) VALUES (%s)", (STATIC_TEXT,))
    conn.commit()
    cur.close()
    conn.close()
    print("[DB] MySQL connected and table ready")


@app.get("/health", tags=["system"])
def health():
    return {"status": "ok"}


@app.get("/message", tags=["message"], response_class=PlainTextResponse)
def get_message():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT content FROM messages LIMIT 1")
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else "No message"