#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
集成测试脚本：验证实时监听模式的端到端功能
流程：
  1. 后台启动 causal-log-analyzer serve
  2. 等待服务健康检查通过
  3. 注册监控因果对
  4. 推送正常日志（阶段1：基线期）
  5. 实时查询验证低置信度
  6. 推送含异常模式的日志（阶段2：异常注入期）
  7. 实时查询验证高置信度
  8. 监听并验证告警触发
  9. 关闭服务
"""

import json
import time
import subprocess
import asyncio
import aiohttp
import websockets
import sys
import os
import signal
from datetime import datetime

if os.name == "nt":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

PYTHON = sys.executable
SERVER_URL = "http://localhost:8766"
WS_URL = "ws://localhost:8766"
CAUSE = "db_query: SELECT * FROM users"
EFFECT = "http_call: 500 Internal Server Error"

test_results = []
server_proc = None
push_proc = None


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def check_passed(name):
    log(f"✓ 测试通过: {name}")
    test_results.append((name, True))


def check_failed(name, detail=""):
    log(f"✗ 测试失败: {name} - {detail}")
    test_results.append((name, False))


async def wait_for_health(timeout: int = 30):
    async with aiohttp.ClientSession() as session:
        start = time.time()
        while time.time() - start < timeout:
            try:
                async with session.get(f"{SERVER_URL}/api/health") as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("status") == "ok":
                            return True
            except Exception:
                pass
            await asyncio.sleep(1)
    return False


async def test_query(expected_score_range: tuple, label: str):
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{SERVER_URL}/api/query", json={
            "cause": CAUSE,
            "effect": EFFECT,
        }) as resp:
            if resp.status != 200:
                return None, await resp.text()
            data = await resp.json()
            score = data.get("confidence_score", -1)
            co_occ = data.get("co_occurrence_traces", -1)
            log(f"  [{label}] 置信度={score:.1f}%, 共现={co_occ} traces")
            if expected_score_range:
                low, high = expected_score_range
                if low <= score <= high:
                    check_passed(f"{label} 置信度在预期范围内 ({low}-{high})")
                else:
                    check_failed(f"{label} 置信度超出预期", f"score={score}, 预期 {low}-{high}")
            return data, None


async def test_watch_pair():
    async with aiohttp.ClientSession() as session:
        async with session.post(f"{SERVER_URL}/api/watch", json={
            "cause": CAUSE,
            "effect": EFFECT,
        }) as resp:
            if resp.status == 200:
                check_passed("注册监控对成功")
            else:
                check_failed("注册监控对", f"status={resp.status}")

        async with session.get(f"{SERVER_URL}/api/watch") as resp:
            data = await resp.json()
            pairs = data.get("pairs", [])
            if any(p["cause"] == CAUSE and p["effect"] == EFFECT for p in pairs):
                check_passed("监控对在列表中")
            else:
                check_failed("监控对在列表中", f"pairs={pairs}")


async def test_listen_for_alerts(duration: int):
    alerts_received = []
    try:
        async with websockets.connect(f"{WS_URL}/ws/alerts") as ws:
            await ws.send(json.dumps({"action": "watch", "cause": CAUSE, "effect": EFFECT}))
            log("  已连接告警 WebSocket，等待告警...")

            start = time.time()
            while time.time() - start < duration:
                try:
                    msg = await asyncio.wait_for(ws.recv(), timeout=1.0)
                    data = json.loads(msg)
                    if data.get("type") == "alert":
                        alert = data["data"]
                        alerts_received.append(alert)
                        log(f"  !! 收到告警: {alert['alert_type']} - {alert['cause']} -> {alert['effect']}")
                        log(f"     分数: {alert['old_score']:.1f}% -> {alert['new_score']:.1f}% (Δ {alert['change_magnitude']:+.1f})")
                        log(f"     {alert['message'][:100]}...")
                        break
                except asyncio.TimeoutError:
                    continue
    except Exception as e:
        log(f"  告警监听连接异常: {e}")

    return alerts_received


async def run_tests():
    global server_proc, push_proc

    print("=" * 70)
    print("实时模式集成测试")
    print("=" * 70)
    print()

    # 1. 启动服务
    log("步骤1: 启动实时服务...")
    server_cmd = [
        PYTHON, "main.py", "serve",
        "--host", "127.0.0.1",
        "--port", "8766",
        "--window-seconds", "120",
        "--sample-interval", "2.0",
        "--min-support", "3",
    ]
    server_proc = subprocess.Popen(
        server_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1,
    )
    await asyncio.sleep(3)

    if server_proc.poll() is not None:
        out, err = server_proc.communicate()
        check_failed("服务启动", f"退出码={server_proc.returncode}, stderr={err[:500]}")
        return

    # 2. 等待健康检查
    log("步骤2: 等待服务就绪...")
    healthy = await wait_for_health(timeout=30)
    if not healthy:
        check_failed("服务健康检查", "超时")
        return
    check_passed("服务健康检查通过")

    # 3. 注册监控
    log("步骤3: 注册监控因果对...")
    await test_watch_pair()

    # 4. 阶段1：推送正常日志
    log("\n步骤4: 推送正常日志（阶段1: 建立基线）...")
    push_proc = subprocess.Popen(
        [PYTHON, "push_client.py",
         "-s", WS_URL,
         "-m", "generate",
         "-n", "600",
         "-i", "20",
         "--anomaly-start", "1000000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    await asyncio.sleep(20)

    log("步骤5: 查询基线置信度...")
    await test_query((0, 50), "基线期")

    # 5. 阶段2：推送含异常的日志
    log("\n步骤6: 推送含异常模式的日志（阶段2: 异常注入）...")
    if push_proc and push_proc.poll() is None:
        push_proc.terminate()
        try:
            push_proc.wait(timeout=5)
        except Exception:
            pass

    push_proc = subprocess.Popen(
        [PYTHON, "push_client.py",
         "-s", WS_URL,
         "-m", "generate",
         "-n", "1200",
         "-i", "20",
         "--anomaly-start", "1",
         "--anomaly-ratio", "0.9"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    await asyncio.sleep(15)

    log("步骤7: 查询异常期置信度...")
    await test_query((70, 100), "异常注入期")

    # 6. 监听告警
    log("\n步骤8: 监听异常告警（最多 60 秒）...")
    alerts = await test_listen_for_alerts(60)

    if alerts:
        check_passed(f"成功收到告警 ({len(alerts)} 条)")
        for a in alerts:
            if a["change_magnitude"] >= 50:
                check_passed("告警幅度 > 50 分阈值")
            else:
                check_failed("告警幅度", f"只有 {a['change_magnitude']} 分")
            if a["new_score"] >= 80:
                check_passed("告警触发时置信度 >= 80%")
            else:
                check_failed("告警触发时置信度", f"只有 {a['new_score']}%")
    else:
        check_failed("告警监听", "60秒内未收到任何告警")

    # 7. 查看最终状态
    log("\n步骤9: 查看最终引擎状态...")
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{SERVER_URL}/api/health") as resp:
            data = await resp.json()
            log(f"  总事件数: {data.get('ingest_events', 'N/A')}")
            log(f"  活跃 traces: {data.get('engine', {}).get('active_traces', 'N/A')}")
            check_passed("引擎仍在运行")

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{SERVER_URL}/api/pair_state",
                               params={"cause": CAUSE, "effect": EFFECT}) as resp:
            if resp.status == 200:
                st = await resp.json()
                log(f"  监控对状态: 基线均值={st['baseline_mean']:.1f}, "
                    f"CUSUM={st['cusum_pos']:.1f}, 连续突破={st['consec_breaches']}")
            else:
                log(f"  无法获取监控对状态: {resp.status}")

    # 8. 清理
    log("\n步骤10: 清理并停止服务...")
    if push_proc and push_proc.poll() is None:
        push_proc.terminate()
        try:
            push_proc.wait(timeout=5)
        except Exception:
            pass

    if server_proc and server_proc.poll() is None:
        server_proc.terminate()
        try:
            server_proc.wait(timeout=10)
        except Exception:
            server_proc.kill()

    # 报告
    print("\n" + "=" * 70)
    print("测试总结")
    print("=" * 70)
    passed = sum(1 for _, ok in test_results if ok)
    total = len(test_results)
    for name, ok in test_results:
        status = "✓ PASS" if ok else "✗ FAIL"
        print(f"  {status}: {name}")
    print(f"\n总计: {passed}/{total} 测试通过")
    if passed == total:
        print("\n🎉 所有测试通过！实时模式功能正常。")
        return 0
    else:
        print(f"\n⚠️  {total - passed} 个测试失败，请检查。")
        return 1


def cleanup():
    global server_proc, push_proc
    for p in [push_proc, server_proc]:
        if p and p.poll() is None:
            try:
                p.terminate()
            except Exception:
                pass
            try:
                p.wait(timeout=3)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass


def signal_handler(sig, frame):
    log("\n收到中断信号，正在清理...")
    cleanup()
    sys.exit(130)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    try:
        code = asyncio.run(run_tests())
        cleanup()
        sys.exit(code)
    except Exception as e:
        log(f"测试异常: {e}")
        cleanup()
        sys.exit(1)


if __name__ == "__main__":
    main()
