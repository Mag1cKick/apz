from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="config-server", version="1.0.0")

registry: Dict[str, List[str]] = defaultdict(list)


class RegisterRequest(BaseModel):
    name: str
    url: str


@app.post("/register")
def register(req: RegisterRequest):
    if req.url not in registry[req.name]:
        registry[req.name].append(req.url)
        print(f"[REG] {req.name} -> {req.url}")
    return {"ok": True, "name": req.name, "url": req.url}


@app.get("/services/{name}")
def get_services(name: str):
    urls = registry.get(name, [])
    if not urls:
        raise HTTPException(status_code=404, detail=f"No instances registered for '{name}'")
    return urls


@app.get("/health")
def health():
    return {"status": "ok", "registry": dict(registry)}
