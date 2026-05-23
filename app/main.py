from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import asyncpg, json, os

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

DB_URL = os.getenv("DATABASE_URL")
active_ws: list[WebSocket] = []
node_last_seen = {"kel1": None, "kel2": None}

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

class IngestPayload(BaseModel):
    group1: dict
    group2: dict
    timestamp: int | None = None

@app.post("/api/ingest")
async def ingest(payload: IngestPayload):
    conn = await get_db()
    row = await conn.fetchrow("""
        INSERT INTO sensor_readings (suhu, kelembapan, lux)
        VALUES ($1, $2, $3)
        RETURNING id, suhu, kelembapan, lux, created_at
    """,
        payload.group1.get("suhu"),
        payload.group1.get("kelembapan"),
        payload.group2.get("lux")
    )
    await conn.close()

    # Update node last seen hanya kalau data non-zero
    if payload.group1.get("suhu", 0) > 0 or payload.group1.get("kelembapan", 0) > 0:
        node_last_seen["kel1"] = datetime.utcnow().isoformat()
    if payload.group2.get("lux", 0) > 0:
        node_last_seen["kel2"] = datetime.utcnow().isoformat()

    data = {
        "suhu": row["suhu"],
        "kelembapan": row["kelembapan"],
        "lux": row["lux"],
        "created_at": str(row["created_at"])
    }

    for ws in active_ws:
        try:
            await ws.send_text(json.dumps(data))
        except:
            pass

    return {"status": "ok", "id": row["id"]}

@app.get("/api/sensor/latest")
async def latest():
    conn = await get_db()
    row = await conn.fetchrow(
        "SELECT * FROM sensor_readings ORDER BY created_at DESC LIMIT 1"
    )
    await conn.close()
    return dict(row) if row else {}

@app.get("/api/sensor/history")
async def history(hours: int = 1):
    conn = await get_db()
    rows = await conn.fetch("""
        SELECT suhu, kelembapan, lux, created_at
        FROM sensor_readings
        WHERE created_at >= NOW() - ($1 || ' hours')::interval
        ORDER BY created_at ASC
    """, str(hours))
    await conn.close()
    return [dict(r) for r in rows]

@app.get("/api/node/status")
async def node_status():
    return node_last_seen

@app.get("/api/daily/report")
async def daily_report():
    conn = await get_db()
    rows = await conn.fetch("""
        SELECT
            DATE(created_at) as tanggal,
            ROUND(AVG(suhu)::numeric, 1)        as avg_suhu,
            ROUND(MIN(suhu)::numeric, 1)        as min_suhu,
            ROUND(MAX(suhu)::numeric, 1)        as max_suhu,
            ROUND(AVG(kelembapan)::numeric, 1)  as avg_kelembapan,
            ROUND(AVG(lux)::numeric, 0)         as avg_lux,
            ROUND(MAX(lux)::numeric, 0)         as max_lux,
            COUNT(*)                             as total_data
        FROM sensor_readings
        WHERE created_at >= NOW() - '7 days'::interval
        GROUP BY DATE(created_at)
        ORDER BY tanggal DESC
    """)
    await conn.close()
    return [dict(r) for r in rows]

@app.websocket("/ws/live")
async def ws_live(ws: WebSocket):
    await ws.accept()
    active_ws.append(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        active_ws.remove(ws)

from fastapi.staticfiles import StaticFiles
app.mount("/", StaticFiles(directory="/app", html=True), name="static")
