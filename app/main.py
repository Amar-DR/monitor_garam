from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import asyncpg, asyncio, json, os

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DB_URL = os.getenv("DATABASE_URL")
active_ws: list[WebSocket] = []

# ── DB init ──
async def get_db():
    return await asyncpg.connect(DB_URL)

@app.on_event("startup")
async def startup():
    conn = await get_db()
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS sensor_readings (
            id          SERIAL PRIMARY KEY,
            suhu        FLOAT,
            kelembapan  FLOAT,
            lux         FLOAT,
            created_at  TIMESTAMP DEFAULT NOW()
        )
    """)
    await conn.close()

# ── Schema ──
class IngestPayload(BaseModel):
    group1: dict  # {suhu, kelembapan}
    group2: dict  # {lux}
    timestamp: int | None = None

# ── POST /api/ingest — dari ESP6 ──
@app.post("/api/ingest")
async def ingest(payload: IngestPayload):
    conn = await get_db()
    row = await conn.fetchrow("""
        INSERT INTO sensor_readings (suhu, kelembapan, lux)
        VALUES ($1, $2, $3)
        RETURNING id, suhu, kelembapan, lux, created_at
    """, payload.group1.get("suhu"), payload.group1.get("kelembapan"),
        payload.group2.get("lux"))
    await conn.close()

    data = {
        "suhu": row["suhu"], "kelembapan": row["kelembapan"],
        "lux": row["lux"], "created_at": str(row["created_at"])
    }
    # Broadcast ke semua browser yang connect
    for ws in active_ws:
        await ws.send_text(json.dumps(data))

    return {"status": "ok", "id": row["id"]}

# ── GET /api/sensor/latest ──
@app.get("/api/sensor/latest")
async def latest():
    conn = await get_db()
    row = await conn.fetchrow(
        "SELECT * FROM sensor_readings ORDER BY created_at DESC LIMIT 1"
    )
    await conn.close()
    return dict(row) if row else {}

# ── GET /api/sensor/history ──
@app.get("/api/sensor/history")
async def history(hours: int = 1):
    conn = await get_db()
    rows = await conn.fetch("""
        SELECT suhu, kelembapan, lux, created_at
        FROM sensor_readings
        WHERE created_at >= NOW() - $1::interval
        ORDER BY created_at ASC
    """, f"{hours} hours")
    await conn.close()
    return [dict(r) for r in rows]

# ── WebSocket /ws/live ──
@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    active_ws.append(ws)
    try:
        while True:
            await ws.receive_text()  # keep-alive
    except WebSocketDisconnect:
        active_ws.remove(ws)
from fastapi.staticfiles import StaticFiles
app.mount("/", StaticFiles(directory="/app", html=True), name="static")
