#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
模拟日志推送客户端 - 通过 WebSocket 向实时服务推送日志
用于测试和验证实时模式
支持两种模式：
  1. replay: 读取已有日志文件，按时间节奏重播
  2. generate: 实时生成模拟日志，可模拟异常注入
"""

import json
import time
import asyncio
import argparse
import uuid
import random
import sys
import os
import websockets

if os.name == "nt":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

SERVICES = ["api-gateway", "user-service", "order-service", "payment-service", "inventory-service"]

EVENT_TYPES = [
    "request_start", "request_end", "db_query", "http_call",
    "cache_hit", "cache_miss", "retry", "timeout", "error", "info",
]

NORMAL_MESSAGES = {
    "request_start": ["GET /api/users", "GET /api/orders", "POST /api/orders", "PUT /api/users/profile"],
    "db_query": ["SELECT * FROM users WHERE id = ?", "SELECT * FROM orders WHERE user_id = ?", "INSERT INTO orders (user_id, status) VALUES (?, ?)", "SELECT * FROM inventory WHERE product_id = ?"],
    "http_call": ["call user-service /validate", "call payment-service /charge", "call inventory-service /reserve", "200 OK"],
    "request_end": ["200 OK"],
    "cache_hit": ["redis: user_cache", "redis: order_cache"],
    "cache_miss": ["redis: user_cache", "redis: order_cache"],
    "info": ["processing request", "validation passed"],
}

ANOMALY_MESSAGES = {
    "db_query": ["SELECT * FROM users"],
    "http_call": ["500 Internal Server Error", "429 Too Many Requests"],
    "timeout": ["db_query timeout (30s)", "http_call timeout (5s)"],
    "request_end": ["500 Internal Server Error"],
    "error": ["database connection lost", "service unavailable"],
}


def _make_entry(trace_id: str, span_id: str, parent_span_id: str,
                event_type: str, message: str, ts: float, service: str) -> dict:
    return {
        "timestamp": round(ts, 6),
        "service_name": service,
        "trace_id": trace_id,
        "span_id": span_id,
        "parent_span_id": parent_span_id,
        "event_type": event_type,
        "message": message,
    }


def generate_normal_trace(trace_id: str, start_ts: float) -> list:
    events = []
    n_steps = random.randint(4, 8)
    current_service = random.choice(SERVICES)
    parent_span_id = ""
    ts = start_ts

    for step in range(n_steps):
        span_id = f"{trace_id}-{step + 1:04d}"
        event_type = random.choice([t for t in EVENT_TYPES if t in NORMAL_MESSAGES])
        message = random.choice(NORMAL_MESSAGES[event_type])
        offset_ms = random.randint(10, 100)
        ts += offset_ms / 1000.0

        events.append(_make_entry(
            trace_id, span_id, parent_span_id,
            event_type, message, ts, current_service,
        ))
        parent_span_id = span_id

    return events


def generate_anomaly_trace(trace_id: str, start_ts: float, anomaly_intensity: float = 0.9) -> list:
    events = generate_normal_trace(trace_id, start_ts)

    if random.random() < anomaly_intensity:
        ts = start_ts + random.randint(100, 500) / 1000.0
        span_id = f"{trace_id}-{len(events) + 1:04d}"
        parent_span_id = events[-1]["span_id"] if events else ""

        events.append(_make_entry(
            trace_id, span_id, parent_span_id,
            "db_query", "SELECT * FROM users",
            ts, random.choice(SERVICES),
        ))

        if random.random() < anomaly_intensity:
            ts2 = ts + random.randint(150, 350) / 1000.0
            span_id2 = f"{trace_id}-{len(events) + 1:04d}"
            events.append(_make_entry(
                trace_id, span_id2, span_id,
                "http_call", "500 Internal Server Error",
                ts2, random.choice(SERVICES),
            ))

    return events


async def generate_and_push(
    ws_url: str,
    total_traces: int,
    interval_ms: int,
    anomaly_ratio: float,
    anomaly_start_at: int,
):
    async with websockets.connect(ws_url) as ws:
        print(f"[push-client] 已连接到 {ws_url}", flush=True)
        welcome = await ws.recv()
        print(f"[push-client] 服务端: {welcome[:100]}", flush=True)

        trace_counter = 0
        anomaly_enabled = False

        while trace_counter < total_traces:
            trace_counter += 1
            trace_id = f"realtime-{trace_counter:08d}-{uuid.uuid4().hex[:6]}"
            start_ts = time.time()

            if trace_counter >= anomaly_start_at and not anomaly_enabled:
                anomaly_enabled = True
                print(f"\n[push-client] === 已注入 {trace_counter} 条 trace，开始注入异常模式！===\n", flush=True)

            if anomaly_enabled and random.random() < anomaly_ratio:
                events = generate_anomaly_trace(trace_id, start_ts)
                tag = "ANOMALY"
            else:
                events = generate_normal_trace(trace_id, start_ts)
                tag = "NORMAL"

            payload = json.dumps(events, ensure_ascii=False)
            await ws.send(payload)

            if trace_counter % 100 == 0:
                print(f"[push-client] 已推送 {trace_counter}/{total_traces} traces "
                      f"(当前模式: {tag}, 异常注入: {'ON' if anomaly_enabled else 'OFF'})", flush=True)

            await asyncio.sleep(interval_ms / 1000.0)

        print(f"\n[push-client] 推送完成，共 {trace_counter} traces，等待 5 秒后断开...", flush=True)
        await asyncio.sleep(5)


async def replay_file(ws_url: str, log_file: str, speed: float = 1.0):
    print(f"[push-client] 读取日志文件: {log_file}", flush=True)
    entries = []
    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    print(f"[push-client] 共 {len(entries)} 条日志，按 {speed}x 速度重播", flush=True)

    entries.sort(key=lambda e: e.get("timestamp", 0))
    if len(entries) >= 2:
        time_range = entries[-1]["timestamp"] - entries[0]["timestamp"]
    else:
        time_range = 0

    async with websockets.connect(ws_url) as ws:
        await ws.recv()
        print(f"[push-client] 已连接到 {ws_url}", flush=True)

        batch_size = 50
        for i in range(0, len(entries), batch_size):
            batch = entries[i:i + batch_size]
            payload = json.dumps(batch, ensure_ascii=False)
            await ws.send(payload)

            if (i + batch_size) % 500 == 0:
                print(f"[push-client] 已推送 {i + batch_size}/{len(entries)} 条", flush=True)

            if time_range > 0 and len(entries) > 1:
                idx = min(i + batch_size - 1, len(entries) - 1)
                if idx > 0:
                    sim_elapsed = (entries[idx]["timestamp"] - entries[0]["timestamp"]) / speed
                    real_elapsed = time.time() - entries[0]["timestamp"]
                    wait = max(0, sim_elapsed - real_elapsed)
                    if wait > 0:
                        await asyncio.sleep(min(wait, 0.5))
                    else:
                        await asyncio.sleep(0.01)
                else:
                    await asyncio.sleep(0.01)
            else:
                await asyncio.sleep(0.01)

        print(f"\n[push-client] 重播完成，共 {len(entries)} 条", flush=True)


def main():
    parser = argparse.ArgumentParser(description="模拟日志推送客户端")
    parser.add_argument("-s", "--server", type=str, default="ws://localhost:8765", help="WebSocket 服务地址")
    parser.add_argument("-m", "--mode", type=str, choices=["generate", "replay"], default="generate", help="运行模式")
    parser.add_argument("-f", "--file", type=str, default=None, help="replay 模式下的日志文件")
    parser.add_argument("-n", "--num-traces", type=int, default=3000, help="generate 模式下的 trace 数量")
    parser.add_argument("-i", "--interval", type=int, default=10, help="每条 trace 的推送间隔 (ms)")
    parser.add_argument("--speed", type=float, default=10.0, help="replay 模式下的速度倍率")
    parser.add_argument("--anomaly-ratio", type=float, default=0.85, help="异常模式下注入异常的概率")
    parser.add_argument("--anomaly-start", type=int, default=1000, help="从第 N 条 trace 开始注入异常")
    args = parser.parse_args()

    ws_url = f"{args.server.rstrip('/')}/ws/ingest"

    try:
        if args.mode == "generate":
            asyncio.run(generate_and_push(
                ws_url,
                total_traces=args.num_traces,
                interval_ms=args.interval,
                anomaly_ratio=args.anomaly_ratio,
                anomaly_start_at=args.anomaly_start,
            ))
        else:
            if not args.file:
                print("错误: replay 模式必须指定 --file", file=sys.stderr)
                sys.exit(1)
            asyncio.run(replay_file(ws_url, args.file, speed=args.speed))
    except KeyboardInterrupt:
        print("\n[push-client] 已停止", flush=True)
    except Exception as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
