"""
外部排序模块 - 针对海量日志（10GB+）按 trace_id 排序
策略：
  1. 分块读取（每块最多 chunk_size 行），在内存中按 (trace_id, timestamp) 排序后写入临时文件
  2. K路归并：使用 heapq.merge 合并所有临时文件，输出全局有序流
"""

import os
import json
import tempfile
import heapq
from typing import Iterator, Optional, List, Tuple
import sys


def _json_encode_sort_key(entry: dict) -> Tuple[str, float, str]:
    trace_id = str(entry.get("trace_id", ""))
    ts = float(entry.get("timestamp", 0))
    span_id = str(entry.get("span_id", ""))
    return (trace_id, ts, span_id)


def _iter_lines(filepath: Optional[str], chunk_size_bytes: int) -> Iterator[str]:
    if filepath:
        fh = open(filepath, "r", encoding="utf-8", buffering=1024 * 1024)
    else:
        fh = sys.stdin

    try:
        buf = ""
        while True:
            data = fh.read(chunk_size_bytes)
            if not data:
                if buf:
                    yield buf
                break
            buf += data
            last_nl = buf.rfind("\n")
            if last_nl >= 0:
                lines = buf[:last_nl].splitlines(keepends=False)
                for line in lines:
                    yield line
                buf = buf[last_nl + 1:]
    finally:
        if filepath:
            fh.close()


def external_sort_by_trace(
    input_file: Optional[str],
    output_file: Optional[str] = None,
    chunk_size_lines: int = 50000,
    tmp_dir: Optional[str] = None,
    delete_tmp: bool = True,
    verbose: bool = False,
) -> Tuple[str, List[str]]:
    """
    按 (trace_id, timestamp) 对 JSON 日志进行外部排序

    参数:
        input_file: 输入文件路径，None 表示从 stdin 读取
        output_file: 输出文件路径，None 时自动生成
        chunk_size_lines: 每个内存块的最大行数
        tmp_dir: 临时文件目录
        delete_tmp: 完成后是否删除临时文件
        verbose: 是否打印进度

    返回:
        (输出文件路径, 临时文件路径列表)
    """
    tmp_dir = tmp_dir or tempfile.gettempdir()
    os.makedirs(tmp_dir, exist_ok=True)

    tmp_files: List[str] = []

    current_chunk: List[dict] = []
    chunk_idx = 0
    parsed_count = 0
    skipped_count = 0

    if verbose:
        sys.stderr.write("[external_sort] 开始分块读取与排序...\n")

    for line in _iter_lines(input_file, chunk_size_bytes=4 * 1024 * 1024):
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
            current_chunk.append(entry)
            parsed_count += 1
        except (json.JSONDecodeError, ValueError):
            skipped_count += 1
            continue

        if len(current_chunk) >= chunk_size_lines:
            tmp_path = _write_sorted_chunk(current_chunk, tmp_dir, chunk_idx)
            tmp_files.append(tmp_path)
            chunk_idx += 1
            current_chunk.clear()
            if verbose:
                sys.stderr.write(f"  已处理 {parsed_count} 行, 生成 {chunk_idx} 个临时块...\n")

    if current_chunk:
        tmp_path = _write_sorted_chunk(current_chunk, tmp_dir, chunk_idx)
        tmp_files.append(tmp_path)
        chunk_idx += 1
        current_chunk.clear()

    if verbose:
        sys.stderr.write(
            f"[external_sort] 读取完成: {parsed_count} 有效行, {skipped_count} 无效行, "
            f"{chunk_idx} 个临时块\n"
        )
        sys.stderr.write("[external_sort] 开始 K 路归并...\n")

    if output_file is None:
        output_fd, output_file = tempfile.mkstemp(
            suffix="_sorted.log", prefix="causal_sort_", dir=tmp_dir
        )
        os.close(output_fd)

    iterators = [_iter_sorted_file(path) for path in tmp_files]

    with open(output_file, "w", encoding="utf-8", buffering=1024 * 1024) as out_fh:
        for entry in heapq.merge(*iterators, key=_json_encode_sort_key):
            out_fh.write(json.dumps(entry, ensure_ascii=False) + "\n")

    for it in iterators:
        try:
            it.close()
        except Exception:
            pass

    if delete_tmp:
        for path in tmp_files:
            try:
                os.remove(path)
            except OSError:
                pass
        tmp_files = []

    if verbose:
        sys.stderr.write(f"[external_sort] 完成! 输出文件: {output_file}\n")

    return output_file, tmp_files


def _write_sorted_chunk(chunk: List[dict], tmp_dir: str, idx: int) -> str:
    chunk.sort(key=_json_encode_sort_key)
    fd, tmp_path = tempfile.mkstemp(
        suffix=f"_chunk_{idx:06d}.log", prefix="causal_sort_", dir=tmp_dir
    )
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        for entry in chunk:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return tmp_path


def _iter_sorted_file(filepath: str) -> Iterator[dict]:
    with open(filepath, "r", encoding="utf-8", buffering=1024 * 1024) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue


def iter_grouped_traces(sorted_file: str) -> Iterator[Tuple[str, List[dict]]]:
    """
    遍历已排序文件，按 trace_id 分组产出事件列表
    仅在内存中保留当前 trace 的数据
    """
    current_trace_id = None
    current_events: List[dict] = []

    with open(sorted_file, "r", encoding="utf-8", buffering=1024 * 1024) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue

            trace_id = str(entry.get("trace_id", ""))
            if not trace_id:
                continue

            if current_trace_id is None:
                current_trace_id = trace_id
                current_events.append(entry)
            elif trace_id == current_trace_id:
                current_events.append(entry)
            else:
                yield (current_trace_id, current_events)
                current_trace_id = trace_id
                current_events = [entry]

    if current_trace_id is not None:
        yield (current_trace_id, current_events)
