"""
实时服务端 - FastAPI + WebSocket
提供以下接口：

WebSocket:
  /ws/ingest        - 接收实时日志流（单向：客户端 -> 服务端）
  /ws/alerts        - 告警推送（单向：服务端 -> 客户端）

HTTP:
  GET  /api/health                  - 健康检查
  GET  /api/stats                   - 获取引擎统计信息
  POST /api/query                   - 查询实时置信度 {cause, effect}
  POST /api/watch                   - 注册监控对 {cause, effect}
  DELETE /api/watch                 - 取消监控 {cause, effect}
  GET  /api/watch                   - 列出所有监控对
  GET  /api/alerts?limit=50&since=0 - 查询历史告警
  GET  /api/pair_state?cause=X&effect=Y - 查询监控对的内部状态
"""

import json
import asyncio
import time
import os
import sys
from typing import Optional, List, Dict
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

if os.name == "nt":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from .incremental_engine import StreamingIncrementalEngine
from .anomaly_detection import AnomalyDetector, AnomalyAlert


class QueryRequest(BaseModel):
    cause: str
    effect: str


class WatchRequest(BaseModel):
    cause: str
    effect: str


class ServerState:
    def __init__(
        self,
        window_seconds: int = 300,
        min_support: int = 2,
        sample_interval: float = 5.0,
    ):
        self.engine = StreamingIncrementalEngine(
            window_seconds=window_seconds,
            min_support=min_support,
        )
        self.detector = AnomalyDetector(sample_interval=sample_interval)
        self.ingest_clients: set = set()
        self.alert_clients: set = set()
        self.ingest_count = 0
        self.start_time = time.time()


state: Optional[ServerState] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global state
    window_seconds = int(os.environ.get("CAUSAL_WINDOW_SECONDS", "300"))
    min_support = int(os.environ.get("CAUSAL_MIN_SUPPORT", "2"))
    sample_interval = float(os.environ.get("CAUSAL_SAMPLE_INTERVAL", "5.0"))

    state = ServerState(window_seconds, min_support, sample_interval)

    await state.engine.start()
    await state.detector.start(state.engine)

    state.detector.register_alert_callback(_broadcast_alert)

    yield

    await state.detector.stop()
    await state.engine.stop()


def _broadcast_alert(alert: AnomalyAlert) -> None:
    global state
    if state is None:
        return

    payload = json.dumps({
        "type": "alert",
        "data": {
            "alert_id": alert.alert_id,
            "timestamp": alert.timestamp,
            "cause": alert.cause,
            "effect": alert.effect,
            "old_score": alert.old_score,
            "new_score": alert.new_score,
            "change_magnitude": alert.change_magnitude,
            "alert_type": alert.alert_type,
            "details": alert.details,
            "message": alert.message,
        },
    }, ensure_ascii=False)

    print(f"[server] alert: {alert.message}", flush=True)

    disconnected = set()
    for ws in state.alert_clients:
        try:
            asyncio.create_task(ws.send_text(payload))
        except Exception:
            disconnected.add(ws)
    for ws in disconnected:
        state.alert_clients.discard(ws)


app = FastAPI(
    title="Causal Log Analyzer - Real-time Server",
    description="分布式链路日志因果链推断 - 实时监听模式",
    version="1.0.0",
    lifespan=lifespan,
)


# ==================== WebSocket 接口 ====================

@app.websocket("/ws/ingest")
async def websocket_ingest(websocket: WebSocket):
    global state
    await websocket.accept()
    if state is None:
        await websocket.close(code=1011, reason="Server not ready")
        return

    state.ingest_clients.add(websocket)
    try:
        await websocket.send_json({
            "type": "welcome",
            "message": "Connected to causal log analyzer ingest endpoint",
            "server_time": time.time(),
        })

        while True:
            try:
                data = await websocket.receive_text()
                entries = _parse_ingest_payload(data)
                if entries:
                    for entry in entries:
                        await state.engine.ingest(entry)
                    state.ingest_count += len(entries)
                    if state.ingest_count % 1000 == 0:
                        print(f"[server] ingested {state.ingest_count} events total", flush=True)
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type": "error",
                    "message": "Invalid JSON payload",
                })
    except WebSocketDisconnect:
        pass
    finally:
        state.ingest_clients.discard(websocket)


def _parse_ingest_payload(data: str) -> List[dict]:
    parsed = json.loads(data)
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        return [parsed]
    return []


@app.websocket("/ws/alerts")
async def websocket_alerts(websocket: WebSocket):
    global state
    await websocket.accept()
    if state is None:
        await websocket.close(code=1011, reason="Server not ready")
        return

    state.alert_clients.add(websocket)
    try:
        await websocket.send_json({
            "type": "welcome",
            "message": "Connected to causal log analyzer alerts endpoint",
            "server_time": time.time(),
        })

        while True:
            try:
                msg = await websocket.receive_text()
                try:
                    parsed = json.loads(msg)
                    action = parsed.get("action")
                    if action == "watch" and "cause" in parsed and "effect" in parsed:
                        await state.detector.watch_pair(parsed["cause"], parsed["effect"])
                        await websocket.send_json({
                            "type": "watch_ack",
                            "cause": parsed["cause"],
                            "effect": parsed["effect"],
                            "watched": True,
                        })
                    elif action == "unwatch" and "cause" in parsed and "effect" in parsed:
                        await state.detector.unwatch_pair(parsed["cause"], parsed["effect"])
                        await websocket.send_json({
                            "type": "unwatch_ack",
                            "cause": parsed["cause"],
                            "effect": parsed["effect"],
                            "watched": False,
                        })
                    elif action == "ping":
                        await websocket.send_json({"type": "pong", "server_time": time.time()})
                except json.JSONDecodeError:
                    pass
            except WebSocketDisconnect:
                break
    finally:
        state.alert_clients.discard(websocket)


# ==================== HTTP 接口 ====================

@app.get("/api/health")
async def health():
    global state
    if state is None:
        raise HTTPException(status_code=503, detail="Server not ready")
    stats = await state.engine.get_stats()
    return {
        "status": "ok",
        "uptime_seconds": round(time.time() - state.start_time, 1),
        "ingest_events": state.ingest_count,
        "ingest_clients": len(state.ingest_clients),
        "alert_clients": len(state.alert_clients),
        "engine": stats,
    }


@app.get("/api/stats")
async def get_stats():
    global state
    if state is None:
        raise HTTPException(status_code=503, detail="Server not ready")
    return await state.engine.get_stats()


@app.post("/api/query")
async def query_causal(req: QueryRequest):
    global state
    if state is None:
        raise HTTPException(status_code=503, detail="Server not ready")
    result = await state.engine.infer(req.cause, req.effect)
    return {
        "mode": "real-time",
        "cause": result.cause,
        "effect": result.effect,
        "confidence_score": result.confidence_score,
        "co_occurrence_traces": result.co_occurrence_traces,
        "total_traces": result.total_traces,
        "avg_time_interval_ms": result.avg_time_interval_ms,
        "std_time_interval_ms": result.std_time_interval_ms,
        "cause_only_traces": result.cause_only_traces,
        "effect_only_traces": result.effect_only_traces,
        "support": result.support,
        "confidence": result.confidence,
        "lift": result.lift,
        "explanation": result.explanation,
    }


@app.post("/api/watch")
async def watch_pair(req: WatchRequest):
    global state
    if state is None:
        raise HTTPException(status_code=503, detail="Server not ready")
    await state.detector.watch_pair(req.cause, req.effect)
    return {
        "status": "ok",
        "cause": req.cause,
        "effect": req.effect,
        "watched": True,
    }


@app.delete("/api/watch")
async def unwatch_pair(req: WatchRequest):
    global state
    if state is None:
        raise HTTPException(status_code=503, detail="Server not ready")
    await state.detector.unwatch_pair(req.cause, req.effect)
    return {
        "status": "ok",
        "cause": req.cause,
        "effect": req.effect,
        "watched": False,
    }


@app.get("/api/watch")
async def list_watched():
    global state
    if state is None:
        raise HTTPException(status_code=503, detail="Server not ready")
    pairs = state.detector.get_watched_pairs()
    return {
        "count": len(pairs),
        "pairs": [{"cause": c, "effect": e} for c, e in pairs],
    }


@app.get("/api/alerts")
async def get_alerts(
    limit: int = Query(50, ge=1, le=500),
    since: Optional[float] = Query(None, ge=0),
):
    global state
    if state is None:
        raise HTTPException(status_code=503, detail="Server not ready")
    alerts = await state.detector.get_recent_alerts(limit=limit, since=since)
    return {
        "count": len(alerts),
        "alerts": [
            {
                "alert_id": a.alert_id,
                "timestamp": a.timestamp,
                "cause": a.cause,
                "effect": a.effect,
                "old_score": a.old_score,
                "new_score": a.new_score,
                "change_magnitude": a.change_magnitude,
                "alert_type": a.alert_type,
                "details": a.details,
                "message": a.message,
            }
            for a in alerts
        ],
    }


@app.get("/api/pair_state")
async def get_pair_state(cause: str, effect: str):
    global state
    if state is None:
        raise HTTPException(status_code=503, detail="Server not ready")
    st = await state.detector.get_pair_state(cause, effect)
    if st is None:
        raise HTTPException(status_code=404, detail="Pair not found")
    return st


@app.exception_handler(HTTPException)
async def http_exception_handler(request, exc):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail},
    )
