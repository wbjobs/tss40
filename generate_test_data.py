#!/usr/bin/env python3
"""
生成用于测试的分布式链路日志数据
包含预设的因果模式，用于验证因果推断工具的正确性
"""

import json
import random
import uuid
import argparse
from datetime import datetime, timedelta

SERVICES = ["api-gateway", "user-service", "order-service", "payment-service", "inventory-service", "notification-service"]

EVENT_TEMPLATES = [
    {"event_type": "request_start", "messages": ["GET /api/users", "POST /api/orders", "GET /api/orders/{id}", "PUT /api/users/profile"]},
    {"event_type": "db_query", "messages": ["SELECT * FROM users WHERE id = ?", "SELECT * FROM orders WHERE user_id = ?", "INSERT INTO orders (user_id, status) VALUES (?, ?)", "UPDATE users SET last_login = NOW() WHERE id = ?", "SELECT * FROM inventory WHERE product_id = ?", "SELECT * FROM payments WHERE order_id = ?", "SELECT * FROM users"]},
    {"event_type": "http_call", "messages": ["call user-service /validate", "call payment-service /charge", "call inventory-service /reserve", "call notification-service /send-email", "500 Internal Server Error", "408 Request Timeout", "404 Not Found", "200 OK", "429 Too Many Requests"]},
    {"event_type": "cache_hit", "messages": ["redis: user_cache", "redis: order_cache", "redis: product_cache"]},
    {"event_type": "cache_miss", "messages": ["redis: user_cache", "redis: order_cache", "redis: product_cache"]},
    {"event_type": "request_end", "messages": ["200 OK", "500 Internal Server Error", "408 Request Timeout", "404 Not Found"]},
    {"event_type": "retry", "messages": ["db_query retry (1/3)", "http_call retry (2/3)"]},
    {"event_type": "timeout", "messages": ["db_query timeout (30s)", "http_call timeout (5s)", "redis timeout (1s)"]},
]

STRONG_CAUSAL_PATTERNS = [
    {
        "cause": ("db_query", "SELECT * FROM users"),
        "effect": ("http_call", "500 Internal Server Error"),
        "probability": 0.75,
        "interval_ms": (150, 350),
        "note": "慢查询导致500错误",
    },
    {
        "cause": ("timeout", "db_query timeout (30s)"),
        "effect": ("http_call", "500 Internal Server Error"),
        "probability": 0.95,
        "interval_ms": (5, 50),
        "note": "DB超时直接导致500",
    },
    {
        "cause": ("http_call", "429 Too Many Requests"),
        "effect": ("request_end", "500 Internal Server Error"),
        "probability": 0.85,
        "interval_ms": (10, 100),
        "note": "限流触发导致请求失败",
    },
    {
        "cause": ("db_query", "SELECT * FROM orders WHERE user_id = ?"),
        "effect": ("cache_miss", "redis: order_cache"),
        "probability": 0.60,
        "interval_ms": (2, 20),
        "note": "查询订单经常缓存未命中",
    },
    {
        "cause": ("cache_hit", "redis: user_cache"),
        "effect": ("request_end", "200 OK"),
        "probability": 0.92,
        "interval_ms": (5, 80),
        "note": "缓存命中通常成功响应",
    },
    {
        "cause": ("request_start", "POST /api/orders"),
        "effect": ("db_query", "INSERT INTO orders (user_id, status) VALUES (?, ?)"),
        "probability": 0.88,
        "interval_ms": (20, 200),
        "note": "创建订单通常执行INSERT",
    },
]

WEAK_CAUSAL_PATTERNS = [
    {
        "cause": ("db_query", "SELECT * FROM users WHERE id = ?"),
        "effect": ("http_call", "call notification-service /send-email"),
        "probability": 0.25,
        "interval_ms": (500, 2000),
        "note": "查询用户后偶尔触发邮件",
    },
    {
        "cause": ("request_start", "GET /api/users"),
        "effect": ("retry", "db_query retry (1/3)"),
        "probability": 0.10,
        "interval_ms": (3000, 5000),
        "note": "查询用户偶尔重试DB",
    },
]


def generate_trace(trace_id: str, base_time: datetime, pattern_weights: dict) -> list:
    entries = []
    current_time = base_time
    span_counter = 1

    num_events = random.randint(4, 12)

    current_service = random.choice(SERVICES[:3])

    for event_idx in range(num_events):
        template = random.choice(EVENT_TEMPLATES)
        event_type = template["event_type"]
        message = random.choice(template["messages"])

        span_id = f"{trace_id}-{span_counter:04d}"
        parent_span_id = f"{trace_id}-{span_counter - 1:04d}" if span_counter > 1 else ""

        offset_ms = random.randint(0, 100)
        current_time = current_time + timedelta(milliseconds=offset_ms)
        timestamp = current_time.timestamp()

        service_choices = [current_service]
        if event_type == "http_call":
            service_choices = SERVICES
        entry_service = random.choice(service_choices)

        entries.append({
            "timestamp": round(timestamp, 6),
            "service_name": entry_service,
            "trace_id": trace_id,
            "span_id": span_id,
            "parent_span_id": parent_span_id,
            "event_type": event_type,
            "message": message,
        })

        if event_type == "http_call" and message.startswith("call"):
            current_service = random.choice(SERVICES)

        span_counter += 1

    for pattern, config in {**{p["note"]: p for p in STRONG_CAUSAL_PATTERNS},
                            **{p["note"]: p for p in WEAK_CAUSAL_PATTERNS}}.items():
        if random.random() < pattern_weights.get(pattern, 0.5):
            cause_type, cause_msg = config["cause"]
            effect_type, effect_msg = config["effect"]
            interval_low, interval_high = config["interval_ms"]

            cause_exists = any(
                e["event_type"] == cause_type and e["message"] == cause_msg
                for e in entries
            )
            effect_exists = any(
                e["event_type"] == effect_type and e["message"] == effect_msg
                for e in entries
            )

            if random.random() < config["probability"]:
                if not cause_exists:
                    insert_pos = random.randint(0, max(0, len(entries) - 2))
                    cause_time = entries[insert_pos]["timestamp"] if insert_pos < len(entries) else base_time.timestamp()
                    span_id = f"{trace_id}-{span_counter:04d}"
                    parent_id = f"{trace_id}-{span_counter - 1:04d}" if span_counter > 1 else ""
                    entries.append({
                        "timestamp": round(cause_time + random.random() * 0.01, 6),
                        "service_name": current_service,
                        "trace_id": trace_id,
                        "span_id": span_id,
                        "parent_span_id": parent_id,
                        "event_type": cause_type,
                        "message": cause_msg,
                    })
                    span_counter += 1

                if not effect_exists and random.random() < config["probability"]:
                    cause_entry = next(
                        (e for e in entries if e["event_type"] == cause_type and e["message"] == cause_msg),
                        None
                    )
                    if cause_entry:
                        interval_ms = random.randint(interval_low, interval_high)
                        effect_time = cause_entry["timestamp"] + (interval_ms / 1000.0)
                        span_id = f"{trace_id}-{span_counter:04d}"
                        parent_id = cause_entry["span_id"]
                        entries.append({
                            "timestamp": round(effect_time, 6),
                            "service_name": random.choice(SERVICES),
                            "trace_id": trace_id,
                            "span_id": span_id,
                            "parent_span_id": parent_id,
                            "event_type": effect_type,
                            "message": effect_msg,
                        })
                        span_counter += 1

    entries.sort(key=lambda e: e["timestamp"])
    return entries


def generate_logs(num_traces: int, output_file: str, seed: int = 42) -> None:
    random.seed(seed)
    base_time = datetime(2026, 6, 19, 8, 0, 0)

    pattern_weights = {p["note"]: 0.7 for p in STRONG_CAUSAL_PATTERNS}
    pattern_weights.update({p["note"]: 0.3 for p in WEAK_CAUSAL_PATTERNS})

    all_lines = []
    for i in range(num_traces):
        trace_id = f"trace-{i + 1:06d}-{uuid.uuid4().hex[:8]}"
        trace_start = base_time + timedelta(seconds=i * random.randint(1, 10))
        trace_entries = generate_trace(trace_id, trace_start, pattern_weights)
        for entry in trace_entries:
            all_lines.append(json.dumps(entry, ensure_ascii=False))

    random.shuffle(all_lines)

    with open(output_file, "w", encoding="utf-8") as f:
        for line in all_lines:
            f.write(line + "\n")

    print(f"已生成 {len(all_lines)} 条日志，共 {num_traces} 个 trace，写入 {output_file}")
    print()
    print("【预设因果模式（用于验证）】")
    for idx, p in enumerate(STRONG_CAUSAL_PATTERNS, 1):
        cause = f'{p["cause"][0]}: {p["cause"][1]}'
        effect = f'{p["effect"][0]}: {p["effect"][1]}'
        print(f"  强模式 #{idx}: {cause}  →  {effect}")
        print(f"            P={p['probability']}, 间隔={p['interval_ms']}ms, {p['note']}")
    print()
    for idx, p in enumerate(WEAK_CAUSAL_PATTERNS, 1):
        cause = f'{p["cause"][0]}: {p["cause"][1]}'
        effect = f'{p["effect"][0]}: {p["effect"][1]}'
        print(f"  弱模式 #{idx}: {cause}  →  {effect}")
        print(f"            P={p['probability']}, 间隔={p['interval_ms']}ms, {p['note']}")


def main():
    parser = argparse.ArgumentParser(description="生成测试用分布式链路日志")
    parser.add_argument("-n", "--num-traces", type=int, default=200, help="生成的 trace 数量（默认 200）")
    parser.add_argument("-o", "--output", type=str, default="app.log", help="输出文件路径（默认 app.log）")
    parser.add_argument("-s", "--seed", type=int, default=42, help="随机种子（默认 42）")
    args = parser.parse_args()

    generate_logs(args.num_traces, args.output, args.seed)


if __name__ == "__main__":
    main()
