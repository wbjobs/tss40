import argparse
import sys
import json
import os
import asyncio
import time
from typing import Optional
from . import __version__
from .parser import LogParser
from .causal_graph import CausalGraph
from .inference import CausalInferenceEngine

if os.name == "nt":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

def _progress_bar(score: float, width: int = 40) -> str:
    filled = int(width * score / 100.0)
    filled = max(0, min(width, filled))
    return "#" * filled + "-" * (width - filled)


class CLI:
    def __init__(self):
        self.parser = self._build_parser()

    def _build_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            prog="causal-log-analyzer",
            description="分布式系统链路日志因果链推断工具 - 通过因果图和频繁模式挖掘推断事件因果关系",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
使用示例:
  %(prog)s analyze --log-file app.log --cause "db_query: SELECT * FROM users" --effect "http_call: 500 Internal Server Error"
  %(prog)s analyze --log-file app.log --list-pairs
  cat app.log | %(prog)s analyze --cause "request_start" --effect "request_end"
            """,
        )

        parser.add_argument(
            "-v", "--version",
            action="version",
            version=f"%(prog)s {__version__}",
        )

        subparsers = parser.add_subparsers(dest="command", required=True)

        # ===== serve 命令：启动实时守护进程 =====
        serve_parser = subparsers.add_parser(
            "serve",
            help="启动实时监听服务（守护进程），接收日志流并维护滑动窗口因果模型",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
示例:
  %(prog)s serve --host 0.0.0.0 --port 8765 --window-seconds 300
  %(prog)s serve -p 8765 -w 600 --sample-interval 3
            """,
        )
        serve_parser.add_argument(
            "-H", "--host",
            type=str,
            default="127.0.0.1",
            help="服务监听地址（默认 127.0.0.1）",
        )
        serve_parser.add_argument(
            "-p", "--port",
            type=int,
            default=8765,
            help="服务监听端口（默认 8765）",
        )
        serve_parser.add_argument(
            "-w", "--window-seconds",
            type=int,
            default=300,
            help="滑动时间窗口大小，单位秒（默认 300 = 5分钟）",
        )
        serve_parser.add_argument(
            "--sample-interval",
            type=float,
            default=5.0,
            help="异常检测的采样间隔，单位秒（默认 5.0）",
        )
        serve_parser.add_argument(
            "--min-support",
            type=int,
            default=2,
            help="置信度计算的最小支持度（默认 2）",
        )
        serve_parser.add_argument(
            "--alert-file",
            type=str,
            default=None,
            help="告警写入的文件路径（默认仅打印到终端）",
        )

        # ===== rt-query 命令：实时查询 =====
        rt_query_parser = subparsers.add_parser(
            "rt-query",
            help="向运行中的实时服务查询当前窗口内的因果置信度",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
示例:
  %(prog)s rt-query --cause "db_query: SELECT * FROM users" --effect "http_call: 500"
  %(prog)s rt-query --server localhost:8765 --cause X --effect Y --json
            """,
        )
        rt_query_parser.add_argument(
            "-s", "--server",
            type=str,
            default="http://localhost:8765",
            help="实时服务地址（默认 http://localhost:8765）",
        )
        rt_query_parser.add_argument(
            "--cause",
            type=str,
            required=True,
            help="原因事件，格式: 'event_type: message'",
        )
        rt_query_parser.add_argument(
            "--effect",
            type=str,
            required=True,
            help="结果事件，格式: 'event_type: message'",
        )
        rt_query_parser.add_argument(
            "--json",
            action="store_true",
            default=False,
            help="以 JSON 格式输出结果",
        )
        rt_query_parser.add_argument(
            "-v", "--verbose",
            action="store_true",
            default=False,
            help="输出详细信息",
        )

        # ===== watch 命令：监听告警 =====
        watch_parser = subparsers.add_parser(
            "watch",
            help="监听实时服务的告警推送，支持注册监控的因果对",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
示例:
  %(prog)s watch --cause "db_query: SELECT * FROM users" --effect "http_call: 500"
  %(prog)s watch --server ws://localhost:8765 --list
  %(prog)s watch --cause X --effect Y --add --output alerts.log
            """,
        )
        watch_parser.add_argument(
            "-s", "--server",
            type=str,
            default="ws://localhost:8765",
            help="实时服务地址（默认 ws://localhost:8765）",
        )
        watch_parser.add_argument(
            "--cause",
            type=str,
            default=None,
            help="要监控的原因事件",
        )
        watch_parser.add_argument(
            "--effect",
            type=str,
            default=None,
            help="要监控的结果事件",
        )
        watch_parser.add_argument(
            "--add",
            action="store_true",
            default=False,
            help="注册监控（连接后发送 watch 命令）",
        )
        watch_parser.add_argument(
            "--remove",
            action="store_true",
            default=False,
            help="取消监控",
        )
        watch_parser.add_argument(
            "--list",
            action="store_true",
            default=False,
            help="仅列出当前监控的因果对，不持续监听",
        )
        watch_parser.add_argument(
            "-o", "--output",
            type=str,
            default=None,
            help="告警写入的文件路径",
        )

        # ===== analyze 命令 =====
        analyze_parser = subparsers.add_parser(
            "analyze",
            help="分析因果关系：推断 cause 事件是否导致 effect 事件",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
示例:
  %(prog)s --log-file app.log --cause "db_query: SELECT" --effect "http_call: 500"
  %(prog)s -f app.log -l 2 --json
            """,
        )

        input_group = analyze_parser.add_mutually_exclusive_group()
        input_group.add_argument(
            "-f", "--log-file",
            type=str,
            default=None,
            help="日志文件路径（每行一个 JSON）。如不指定则从 stdin 读取",
        )

        analyze_parser.add_argument(
            "--cause",
            type=str,
            default=None,
            help='原因事件，格式: "event_type: message"，如 "db_query: SELECT * FROM users"',
        )

        analyze_parser.add_argument(
            "--effect",
            type=str,
            default=None,
            help='结果事件，格式: "event_type: message"，如 "http_call: 500 Internal Server Error"',
        )

        analyze_parser.add_argument(
            "--min-support",
            type=int,
            default=2,
            help="频繁模式的最小支持度（trace 数），默认 2",
        )

        analyze_parser.add_argument(
            "--max-pattern-len",
            type=int,
            default=10,
            help="挖掘的最大序列模式长度，默认 10",
        )

        analyze_parser.add_argument(
            "--list-pairs",
            action="store_true",
            default=False,
            help="列出排名前 N 的因果事件对（无需指定 cause/effect）",
        )

        analyze_parser.add_argument(
            "-n", "--top-n",
            type=int,
            default=20,
            help="--list-pairs 时显示的对数，默认 20",
        )

        analyze_parser.add_argument(
            "--json",
            action="store_true",
            default=False,
            help="以 JSON 格式输出结果",
        )

        analyze_parser.add_argument(
            "-v", "--verbose",
            action="store_true",
            default=False,
            help="输出详细的中间统计信息",
        )

        analyze_parser.add_argument(
            "-s", "--streaming",
            action="store_true",
            default=False,
            help="启用流式处理模式（外部排序 + Count-Min Sketch，适合 10GB+ 海量日志）",
        )

        analyze_parser.add_argument(
            "--chunk-size",
            type=int,
            default=50000,
            help="流式模式下每个内存块的最大行数（默认 50000）",
        )

        analyze_parser.add_argument(
            "--tmp-dir",
            type=str,
            default=None,
            help="流式模式下临时文件目录（默认使用系统临时目录）",
        )

        return parser

    def run(self, argv: Optional[list] = None) -> int:
        args = self.parser.parse_args(argv)

        if args.command == "analyze":
            return self._cmd_analyze(args)
        elif args.command == "serve":
            return self._cmd_serve(args)
        elif args.command == "rt-query":
            return self._cmd_rt_query(args)
        elif args.command == "watch":
            return self._cmd_watch(args)

        self.parser.print_help()
        return 1

    def _cmd_analyze(self, args) -> int:
        if args.streaming:
            return self._cmd_analyze_streaming(args)
        return self._cmd_analyze_in_memory(args)

    def _cmd_analyze_streaming(self, args) -> int:
        if args.list_pairs:
            print("错误：--streaming 模式下仅支持指定 cause/effect 的定向分析，不支持 --list-pairs", file=sys.stderr)
            return 11
        if not args.cause or not args.effect:
            print("错误：流式模式下必须同时指定 --cause 和 --effect", file=sys.stderr)
            return 6

        try:
            from .streaming_engine import StreamingCausalEngine
        except ImportError as e:
            print(f"错误：加载流式引擎失败 - {e}", file=sys.stderr)
            return 12

        if args.verbose:
            sys.stderr.write("[mode] 流式处理模式已启用：外部排序 + Count-Min Sketch + 子图构建\n")

        try:
            with StreamingCausalEngine(
                cause=args.cause,
                effect=args.effect,
                min_support=args.min_support,
                chunk_size_lines=args.chunk_size,
                tmp_dir=args.tmp_dir,
                verbose=args.verbose,
            ) as engine:
                engine.load(args.log_file)
                result = engine.infer()
        except FileNotFoundError:
            print(f"错误：找不到日志文件 {args.log_file}", file=sys.stderr)
            return 2
        except MemoryError:
            print("错误：内存不足，请增大 --chunk-size 或切换到流式模式 --streaming", file=sys.stderr)
            return 13
        except Exception as e:
            print(f"错误：流式处理失败 - {e}", file=sys.stderr)
            return 3

        if args.json:
            output = {
                "mode": "streaming",
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
            print(json.dumps(output, ensure_ascii=False, indent=2))
        else:
            print("=" * 70)
            print("[处理模式: STREAMING (外部排序 + Count-Min Sketch + 子图)]")
            self._print_result(result, args.verbose)

        return 0

    def _cmd_analyze_in_memory(self, args) -> int:
        try:
            entries = self._load_logs(args.log_file)
        except FileNotFoundError:
            print(f"错误：找不到日志文件 {args.log_file}", file=sys.stderr)
            return 2
        except Exception as e:
            print(f"错误：读取日志失败 - {e}", file=sys.stderr)
            return 3

        if not entries:
            print("错误：未解析到任何有效日志条目", file=sys.stderr)
            return 4

        sequences = LogParser.group_by_trace(entries)
        if not sequences:
            print("错误：未找到任何包含 trace_id 的日志条目", file=sys.stderr)
            return 5

        graph = CausalGraph()
        graph.build_from_traces(sequences)

        engine = CausalInferenceEngine(
            graph=graph,
            min_support=args.min_support,
            max_pattern_len=args.max_pattern_len,
        )

        if args.verbose:
            self._print_stats(entries, sequences, graph)

        if args.list_pairs:
            return self._print_top_pairs(engine, args)

        if not args.cause or not args.effect:
            print("错误：必须同时指定 --cause 和 --effect，或使用 --list-pairs", file=sys.stderr)
            return 6

        result = engine.infer(args.cause, args.effect)

        if args.json:
            output = {
                "mode": "in-memory",
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
            print(json.dumps(output, ensure_ascii=False, indent=2))
        else:
            print("=" * 70)
            print("[处理模式: IN-MEMORY (完整因果图 + 精确计数)]")
            self._print_result(result, args.verbose)

        return 0

    @staticmethod
    def _load_logs(log_file: Optional[str]):
        if log_file:
            return LogParser.parse_file(log_file)
        return LogParser.parse_stdin()

    @staticmethod
    def _print_stats(entries, sequences, graph):
        print("=" * 60)
        print("【日志统计概览】")
        print("=" * 60)
        print(f"  总日志行数:       {len(entries)}")
        print(f"  独立 Trace 数:    {len(sequences)}")
        print(f"  独立事件类型数:   {len(graph.nodes)}")
        print(f"  因果边数量:       {len(graph.edges)}")
        print()

        print("【Top 10 高频事件】")
        sorted_nodes = sorted(graph.nodes.values(), key=lambda n: n.count, reverse=True)[:10]
        for node in sorted_nodes:
            trace_pct = len(node.trace_ids) / max(1, graph.total_traces) * 100
            print(f"  {node.key}")
            print(f"    出现次数: {node.count}，覆盖 {len(node.trace_ids)} 个 trace ({trace_pct:.1f}%)")
        print()

    @staticmethod
    def _print_top_pairs(engine: CausalInferenceEngine, args) -> int:
        pairs = engine.get_top_causal_pairs(args.top_n)

        if args.json:
            output = [
                {"cause": c, "effect": e, "score": s}
                for c, e, s in pairs
            ]
            print(json.dumps(output, ensure_ascii=False, indent=2))
        else:
            print("=" * 80)
            print(f"【Top {len(pairs)} 因果事件对】")
            print("=" * 80)
            for rank, (cause, effect, score) in enumerate(pairs, 1):
                bar = _progress_bar(score, 40)
                print(f"  #{rank:2d}  [{bar}] {score:5.1f}%")
                print(f"         原因: {cause}")
                print(f"         结果: {effect}")
                print()

        return 0

    @staticmethod
    def _print_result(result, verbose: bool):
        print("=" * 70)
        print("【因果推断结果】")
        print("=" * 70)
        print(f"  原因事件:  {result.cause}")
        print(f"  结果事件:  {result.effect}")
        score = result.confidence_score
        bar = _progress_bar(score, 40)
        level = "高" if score >= 80 else ("中" if score >= 50 else "低")
        print(f"  置信度:    {score:5.1f}% [{bar}]  ({level}置信度)")
        print()

        print("=" * 70)
        print("【详细说明】")
        print("=" * 70)
        print(f"  {result.explanation}")
        print()

        if verbose:
            print("=" * 70)
            print("【统计指标】")
            print("=" * 70)
            print(f"  共现 Trace 数:       {result.co_occurrence_traces} / {result.total_traces}")
            print(f"  仅含 Cause 的 Trace: {result.cause_only_traces}")
            print(f"  仅含 Effect 的 Trace:{result.effect_only_traces}")
            print(f"  支持度 Support:      {result.support}")
            print(f"  置信度 Confidence:   {result.confidence}")
            print(f"  提升度 Lift:         {result.lift}")
            if result.avg_time_interval_ms > 0:
                print(f"  平均时间间隔:        {result.avg_time_interval_ms} ms")
                print(f"  时间间隔标准差:      ±{result.std_time_interval_ms} ms")
            print()


    def _cmd_serve(self, args) -> int:
        import uvicorn
        from .anomaly_detection import AnomalyAlert

        os.environ["CAUSAL_WINDOW_SECONDS"] = str(args.window_seconds)
        os.environ["CAUSAL_MIN_SUPPORT"] = str(args.min_support)
        os.environ["CAUSAL_SAMPLE_INTERVAL"] = str(args.sample_interval)

        alert_file = args.alert_file
        alert_fh = None

        def alert_handler(alert: AnomalyAlert):
            if alert_file:
                nonlocal alert_fh
                if alert_fh is None:
                    try:
                        alert_fh = open(alert_file, "a", encoding="utf-8")
                    except Exception as e:
                        print(f"[serve] 无法打开告警文件 {alert_file}: {e}", file=sys.stderr)
                        return
                try:
                    alert_fh.write(json.dumps({
                        "alert_id": alert.alert_id,
                        "timestamp": alert.timestamp,
                        "cause": alert.cause,
                        "effect": alert.effect,
                        "old_score": alert.old_score,
                        "new_score": alert.new_score,
                        "change_magnitude": alert.change_magnitude,
                        "alert_type": alert.alert_type,
                        "message": alert.message,
                    }, ensure_ascii=False) + "\n")
                    alert_fh.flush()
                except Exception as e:
                    print(f"[serve] 写入告警文件失败: {e}", file=sys.stderr)

        try:
            from . import realtime_server
            realtime_server._alert_file_handler = alert_handler
        except Exception as e:
            print(f"[serve] 初始化告警处理器失败: {e}", file=sys.stderr)

        print("=" * 70)
        print("[实时模式] 启动因果日志分析实时服务")
        print("=" * 70)
        print(f"  监听地址:     {args.host}:{args.port}")
        print(f"  滑动窗口:     {args.window_seconds} 秒 ({args.window_seconds/60:.1f} 分钟)")
        print(f"  采样间隔:     {args.sample_interval} 秒")
        print(f"  最小支持度:   {args.min_support}")
        if alert_file:
            print(f"  告警文件:     {alert_file}")
        print()
        print("  WebSocket 入口:")
        print(f"    ws://{args.host}:{args.port}/ws/ingest   (接收日志流)")
        print(f"    ws://{args.host}:{args.port}/ws/alerts   (告警推送)")
        print("  HTTP API:")
        print(f"    GET  http://{args.host}:{args.port}/api/health")
        print(f"    GET  http://{args.host}:{args.port}/api/stats")
        print(f"    POST http://{args.host}:{args.port}/api/query")
        print(f"    GET  http://{args.host}:{args.port}/api/watch")
        print(f"    GET  http://{args.host}:{args.port}/api/alerts")
        print("=" * 70)
        print("按 Ctrl+C 停止服务")
        print()

        try:
            uvicorn.run(
                "causal_analyzer.realtime_server:app",
                host=args.host,
                port=args.port,
                log_level="info",
                reload=False,
            )
        except KeyboardInterrupt:
            print("\n[serve] 服务已停止", flush=True)
        except Exception as e:
            print(f"[serve] 服务运行失败: {e}", file=sys.stderr)
            return 14
        finally:
            if alert_fh:
                try:
                    alert_fh.close()
                except Exception:
                    pass

        return 0

    def _cmd_rt_query(self, args) -> int:
        import aiohttp

        server = args.server.rstrip("/")
        url = f"{server}/api/query"

        async def do_query():
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json={
                    "cause": args.cause,
                    "effect": args.effect,
                }) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        print(f"错误: 服务返回 {resp.status}: {text}", file=sys.stderr)
                        return None
                    return await resp.json()

        try:
            result = asyncio.run(do_query())
        except Exception as e:
            print(f"错误: 无法连接到实时服务 {server}: {e}", file=sys.stderr)
            return 15

        if result is None:
            return 15

        if args.json:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print("=" * 70)
            print(f"[实时查询] 服务: {server}")
            self._print_result(result, args.verbose)

        return 0

    def _cmd_watch(self, args) -> int:
        import aiohttp
        import websockets

        ws_server = args.server.rstrip("/").replace("http://", "ws://").replace("https://", "wss://")
        http_server = args.server.rstrip("/").replace("ws://", "http://").replace("wss://", "https://")

        if args.list:
            async def do_list():
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{http_server}/api/watch") as resp:
                        if resp.status != 200:
                            print(f"错误: {resp.status}", file=sys.stderr)
                            return None
                        return await resp.json()
            try:
                data = asyncio.run(do_list())
            except Exception as e:
                print(f"错误: 无法连接到服务 {http_server}: {e}", file=sys.stderr)
                return 15
            if data:
                print(f"当前监控 {data['count']} 个因果对:")
                for i, p in enumerate(data["pairs"], 1):
                    print(f"  #{i:2d}  {p['cause']}  →  {p['effect']}")
            return 0

        if args.add or args.remove:
            if not args.cause or not args.effect:
                print("错误: --add/--remove 需要同时指定 --cause 和 --effect", file=sys.stderr)
                return 6

            method = "POST" if args.add else "DELETE"
            async def do_watch():
                async with aiohttp.ClientSession() as session:
                    async with session.request(method, f"{http_server}/api/watch", json={
                        "cause": args.cause,
                        "effect": args.effect,
                    }) as resp:
                        if resp.status != 200:
                            print(f"错误: {resp.status}", file=sys.stderr)
                            return None
                        return await resp.json()

            try:
                result = asyncio.run(do_watch())
            except Exception as e:
                print(f"错误: 无法连接到服务 {http_server}: {e}", file=sys.stderr)
                return 15
            if result:
                action = "已注册监控" if args.add else "已取消监控"
                print(f"{action}: {result['cause']} → {result['effect']}")
            return 0

        if args.cause and args.effect:
            async def do_register_and_watch():
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(f"{http_server}/api/watch", json={
                            "cause": args.cause,
                            "effect": args.effect,
                        }) as resp:
                            if resp.status != 200:
                                print(f"警告: 注册监控失败: {resp.status}", file=sys.stderr)
                except Exception as e:
                    print(f"警告: 注册监控失败: {e}", file=sys.stderr)

                output_fh = None
                if args.output:
                    try:
                        output_fh = open(args.output, "a", encoding="utf-8")
                    except Exception as e:
                        print(f"警告: 无法打开输出文件 {args.output}: {e}", file=sys.stderr)

                try:
                    ws_url = f"{ws_server}/ws/alerts"
                    async with websockets.connect(ws_url) as ws:
                        print(f"[watch] 已连接到 {ws_url}，等待告警...")
                        print(f"[watch] 监控: {args.cause} → {args.effect}")
                        print("按 Ctrl+C 停止")
                        print()

                        watch_msg = json.dumps({
                            "action": "watch",
                            "cause": args.cause,
                            "effect": args.effect,
                        })
                        await ws.send(watch_msg)

                        while True:
                            try:
                                msg = await ws.recv()
                                data = json.loads(msg)
                                if data.get("type") == "alert":
                                    alert = data["data"]
                                    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(alert["timestamp"]))
                                    level = "!!!" if alert["change_magnitude"] > 60 else "!"
                                    print(f"[{ts}] {level} {alert['alert_type'].upper()} {level}")
                                    print(f"  {alert['message']}")
                                    print(f"  分数变化: {alert['old_score']:.1f}% → {alert['new_score']:.1f}% "
                                          f"(Δ {alert['change_magnitude']:+.1f})")
                                    print()

                                    if output_fh:
                                        try:
                                            output_fh.write(json.dumps(alert, ensure_ascii=False) + "\n")
                                            output_fh.flush()
                                        except Exception:
                                            pass
                                elif data.get("type") == "watch_ack":
                                    print(f"[watch] 确认监控: {data['cause']} → {data['effect']}")
                                    print()
                            except websockets.ConnectionClosed:
                                print("[watch] 连接已断开", file=sys.stderr)
                                break
                finally:
                    if output_fh:
                        try:
                            output_fh.close()
                        except Exception:
                            pass

            try:
                asyncio.run(do_register_and_watch())
            except KeyboardInterrupt:
                print("\n[watch] 已停止")
            except Exception as e:
                print(f"错误: 连接失败: {e}", file=sys.stderr)
                return 15

            return 0

        print("错误: 请指定 --list, 或同时指定 --cause/--effect 进行监控", file=sys.stderr)
        return 6


def main():
    cli = CLI()
    sys.exit(cli.run())


if __name__ == "__main__":
    main()
