from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI(title="messages-service", version="1.0.0")

STATIC_TEXT = "Not implemented yet"


@app.get("/health", tags=["system"])
def health():
    return {"status": "ok"}


@app.get("/message", tags=["message"], response_class=PlainTextResponse)
def get_message():
    return STATIC_TEXT
