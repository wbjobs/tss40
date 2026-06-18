import argparse
import sys
import json
import os
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


def main():
    cli = CLI()
    sys.exit(cli.run())


if __name__ == "__main__":
    main()
