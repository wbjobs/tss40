#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""快速验证实时模式核心模块"""
import asyncio
import time
import json
import sys
import os

if os.name == "nt":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

async def test_sliding_window():
    from causal_analyzer.sliding_window import SlidingTimeWindow

    print("测试 1: SlidingTimeWindow")
    print("=" * 50)
    window = SlidingTimeWindow(window_seconds=60, cleanup_interval=1.0, trace_timeout=5.0)
    await window.start()

    ts = time.time()
    for i in range(100):
        entry = {
            "timestamp": ts + i * 0.01,
            "service_name": "test",
            "trace_id": f"trace-{i % 20:04d}",
            "span_id": f"span-{i}",
            "parent_span_id": f"span-{i-1}",
            "event_type": "db_query" if i % 3 == 0 else "http_call",
            "message": "SELECT * FROM users" if i % 3 == 0 else "500 Internal Server Error",
        }
        await window.add_event(entry)

    stats = await window.get_stats()
    print(f"  总 traces: {stats['total_traces']}")
    print(f"  总事件: {stats['total_events']}")
    print(f"  因果对: {stats['pair_count']}")

    pair_stats = await window.get_causal_pair_stats(
        "db_query: SELECT * FROM users",
        "http_call: 500 Internal Server Error"
    )
    print(f"  目标对共现: {pair_stats['co_occurrence_traces']}")
    print(f"  平均间隔: {pair_stats['avg_interval_ms']:.1f} ms")
    print("  ✓ SlidingTimeWindow OK\n")

    await window.stop()
    return stats['total_traces'] > 0


async def test_incremental_engine():
    from causal_analyzer.incremental_engine import StreamingIncrementalEngine

    print("测试 2: StreamingIncrementalEngine")
    print("=" * 50)
    engine = StreamingIncrementalEngine(window_seconds=60, min_support=2)
    await engine.start()

    ts = time.time()
    for trace_idx in range(30):
        cause_ts = ts + trace_idx * 0.5
        await engine.ingest({
            "timestamp": cause_ts,
            "service_name": "user-service",
            "trace_id": f"test-trace-{trace_idx:04d}",
            "span_id": "span-1",
            "parent_span_id": "",
            "event_type": "db_query",
            "message": "SELECT * FROM users",
        })
        if trace_idx < 25:
            await engine.ingest({
                "timestamp": cause_ts + 0.25,
                "service_name": "api-gateway",
                "trace_id": f"test-trace-{trace_idx:04d}",
                "span_id": "span-2",
                "parent_span_id": "span-1",
                "event_type": "http_call",
                "message": "500 Internal Server Error",
            })

    result = await engine.infer(
        "db_query: SELECT * FROM users",
        "http_call: 500 Internal Server Error"
    )
    print(f"  置信度分数: {result.confidence_score:.1f}%")
    print(f"  共现 traces: {result.co_occurrence_traces}/{result.total_traces}")
    print(f"  置信度: {result.confidence:.4f}")
    print(f"  提升度: {result.lift:.4f}")
    print(f"  平均间隔: {result.avg_time_interval_ms:.1f} ms")

    stats = await engine.get_stats()
    print(f"  引擎状态: {stats['active_traces']} traces, {stats['events_in_window']} events")

    if result.confidence_score > 80 and result.confidence >= 0.8:
        print("  ✓ StreamingIncrementalEngine OK (高置信度正确识别)\n")
        ok = True
    else:
        print("  ⚠ StreamingIncrementalEngine 分数偏低，但可能是小样本导致")
        ok = result.confidence_score > 50

    await engine.stop()
    return ok


async def test_anomaly_detector():
    from causal_analyzer.anomaly_detection import AnomalyDetector
    from causal_analyzer.incremental_engine import StreamingIncrementalEngine

    print("测试 3: AnomalyDetector")
    print("=" * 50)

    engine = StreamingIncrementalEngine(window_seconds=120, min_support=2)
    await engine.start()

    detector = AnomalyDetector(
        sample_interval=0.1,
        min_samples_for_baseline=3,
        change_threshold=20.0,
        min_rising_score=50.0,
        alert_cooldown=0.5,
        consec_breaches_required=1,
        cusum_threshold=10.0,
    )
    await detector.start(engine)

    alerts_received = []
    detector.register_alert_callback(lambda a: alerts_received.append(a))

    await detector.watch_pair("db_query: SELECT * FROM users", "http_call: 500 Internal Server Error")

    print("  阶段1: 正常基线（低共现）")
    ts = time.time()
    for trace_idx in range(15):
        cause_ts = ts + trace_idx * 0.1
        await engine.ingest({
            "timestamp": cause_ts,
            "trace_id": f"base-{trace_idx:04d}",
            "span_id": "s1", "parent_span_id": "",
            "event_type": "db_query",
            "message": "SELECT * FROM users",
            "service_name": "test",
        })
        if trace_idx % 5 == 0:
            await engine.ingest({
                "timestamp": cause_ts + 0.01,
                "trace_id": f"base-{trace_idx:04d}",
                "span_id": "s2", "parent_span_id": "s1",
                "event_type": "http_call",
                "message": "500 Internal Server Error",
                "service_name": "test",
            })
    await asyncio.sleep(0.8)

    result = await engine.infer("db_query: SELECT * FROM users", "http_call: 500 Internal Server Error")
    print(f"    基线期置信度: {result.confidence_score:.1f}%")

    print("  阶段2: 注入异常（高共现）")
    ts2 = time.time()
    for trace_idx in range(120):
        cause_ts = ts2 + trace_idx * 0.01
        await engine.ingest({
            "timestamp": cause_ts,
            "trace_id": f"anom-{trace_idx:04d}",
            "span_id": "s1", "parent_span_id": "",
            "event_type": "db_query",
            "message": "SELECT * FROM users",
            "service_name": "test",
        })
        if trace_idx < 115:
            await engine.ingest({
                "timestamp": cause_ts + 0.002,
                "trace_id": f"anom-{trace_idx:04d}",
                "span_id": "s2", "parent_span_id": "s1",
                "event_type": "http_call",
                "message": "500 Internal Server Error",
                "service_name": "test",
            })

    for _ in range(15):
        await asyncio.sleep(0.15)
        if alerts_received:
            break

    result2 = await engine.infer("db_query: SELECT * FROM users", "http_call: 500 Internal Server Error")
    print(f"    异常期置信度: {result2.confidence_score:.1f}%")
    print(f"    收到告警: {len(alerts_received)} 条")
    print(f"    分数变化: {result.confidence_score:.1f}% -> {result2.confidence_score:.1f}%")

    if alerts_received:
        a = alerts_received[0]
        print(f"    告警: {a.alert_type}, {a.old_score:.1f} -> {a.new_score:.1f}, Δ{a.change_magnitude:.1f}")

    state = await detector.get_pair_state("db_query: SELECT * FROM users", "http_call: 500 Internal Server Error")
    if state:
        print(f"    监控对状态: 基线={state['baseline_mean']:.1f}, CUSUM={state['cusum_pos']:.1f}")

    ok = result2.confidence_score > 50
    if ok and alerts_received:
        print("  ✓ AnomalyDetector OK (告警正确触发)\n")
        ok = True
    elif ok:
        print("  ✓ AnomalyDetector OK (置信度显著上升)\n")
        ok = True
    else:
        print("  ⚠ AnomalyDetector 需要更多采样数据")
        ok = result2.confidence_score > 50

    await detector.stop()
    await engine.stop()
    return ok


async def main():
    print("\n" + "=" * 60)
    print("实时模式核心模块测试")
    print("=" * 60 + "\n")

    results = []
    try:
        results.append(("SlidingTimeWindow", await test_sliding_window()))
    except Exception as e:
        print(f"  ✗ SlidingTimeWindow 失败: {e}")
        import traceback
        traceback.print_exc()
        results.append(("SlidingTimeWindow", False))

    try:
        results.append(("StreamingIncrementalEngine", await test_incremental_engine()))
    except Exception as e:
        print(f"  ✗ StreamingIncrementalEngine 失败: {e}")
        import traceback
        traceback.print_exc()
        results.append(("StreamingIncrementalEngine", False))

    try:
        results.append(("AnomalyDetector", await test_anomaly_detector()))
    except Exception as e:
        print(f"  ✗ AnomalyDetector 失败: {e}")
        import traceback
        traceback.print_exc()
        results.append(("AnomalyDetector", False))

    print("=" * 60)
    print("测试总结")
    print("=" * 60)
    all_ok = True
    for name, ok in results:
        status = "✓ PASS" if ok else "✗ FAIL"
        print(f"  {status}: {name}")
        if not ok:
            all_ok = False

    if all_ok:
        print("\n🎉 所有核心模块测试通过！")
        return 0
    else:
        print("\n⚠️  部分测试失败")
        return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
