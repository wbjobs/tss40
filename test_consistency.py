#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""对比 in-memory 和 streaming 两种模式的一致性"""
import json
import subprocess
import sys
import os

if os.name == "nt":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

PYTHON = sys.executable
TEST_CASES = [
    ("强模式#1: db_query SELECT users -> 500",
     "db_query: SELECT * FROM users",
     "http_call: 500 Internal Server Error"),
    ("强模式#2: db_query timeout -> 500",
     "timeout: db_query timeout (30s)",
     "http_call: 500 Internal Server Error"),
    ("强模式#3: 429 Too Many Requests -> request_end 500",
     "http_call: 429 Too Many Requests",
     "request_end: 500 Internal Server Error"),
    ("强模式#6: POST /api/orders -> INSERT orders",
     "request_start: POST /api/orders",
     "db_query: INSERT INTO orders (user_id, status) VALUES (?, ?)"),
    ("强模式#4: SELECT orders user_id -> cache_miss order_cache",
     "db_query: SELECT * FROM orders WHERE user_id = ?",
     "cache_miss: redis: order_cache"),
    ("弱模式#1: SELECT users id -> send email",
     "db_query: SELECT * FROM users WHERE id = ?",
     "http_call: call notification-service /send-email"),
    ("无因果对: cache_hit product_cache -> request_start GET users",
     "cache_hit: redis: product_cache",
     "request_start: GET /api/users"),
]

def run_cli(args):
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    result = subprocess.run(
        [PYTHON, "main.py"] + args,
        capture_output=True,
        env=env,
    )
    out = result.stdout.decode("utf-8", errors="replace")
    start = out.find("{")
    end = out.rfind("}")
    if start == -1 or end == -1 or end <= start:
        print("  STDERR:", result.stderr.decode("utf-8", errors="replace")[:500])
        print("  STDOUT:", out[:500])
        return None
    try:
        return json.loads(out[start:end + 1])
    except json.JSONDecodeError as e:
        print("  JSON parse error:", e)
        print("  JSON fragment:", out[start:end + 1][:500])
        return None


def main():
    log_file = "big_app.log"
    print("=" * 80)
    print(f"一致性测试 (日志文件: {log_file})")
    print("=" * 80)
    all_ok = True
    for name, cause, effect in TEST_CASES:
        print(f"\n[{name}]")
        print(f"  cause:  {cause}")
        print(f"  effect: {effect}")
        inmem = run_cli([
            "analyze", "-f", log_file,
            "--cause", cause, "--effect", effect, "--json",
        ])
        stream = run_cli([
            "analyze", "-f", log_file,
            "--cause", cause, "--effect", effect,
            "--streaming", "--chunk-size", "3000", "--json",
        ])
        if inmem is None or stream is None:
            print(f"  ❌ 运行失败")
            all_ok = False
            continue

        keys = [
            ("confidence_score", 12.0),
            ("co_occurrence_traces", 0),
            ("total_traces", 0),
            ("avg_time_interval_ms", 0.06),
            ("std_time_interval_ms", 0.10),
            ("support", 0.0005),
            ("confidence", 0.0005),
            ("lift", 0.005),
        ]
        case_ok = True
        for k, tol in keys:
            v1 = inmem.get(k)
            v2 = stream.get(k)
            if isinstance(v1, float) and isinstance(v2, float) and tol > 0:
                if tol < 1:
                    match = abs(v1 - v2) <= max(abs(v1), abs(v2), 1e-9) * tol
                else:
                    match = abs(v1 - v2) <= tol
            else:
                match = v1 == v2
            status = "OK" if match else "MISMATCH"
            if not match:
                case_ok = False
            print(f"  [{status}] {k}: inmem={v1}, stream={v2}")
        if case_ok:
            print(f"  -> PASS")
        else:
            print(f"  -> FAIL")
            all_ok = False

    print("\n" + "=" * 80)
    if all_ok:
        print("所有测试通过 ✅ 两种模式结果完全一致")
    else:
        print("存在不一致的测试 ❌")
        sys.exit(1)


if __name__ == "__main__":
    main()
