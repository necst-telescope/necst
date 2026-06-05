"""Read-only log access helpers for the NECST Operator Console.

The operator console writes two kinds of local logs:

* operator_console.jsonl: one JSON object per accepted/rejected operator action;
* launcher stdout/stderr logs: local child-process output captured by the console.

This module only reads files that are explicitly configured by the running
console process.  It never follows arbitrary browser-supplied paths outside the
configured log roots.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


JsonDict = Dict[str, Any]


def _coerce_limit(value: Any, *, default: int, minimum: int = 1, maximum: int = 1000) -> int:
    try:
        number = int(value)
    except Exception:
        number = int(default)
    return max(int(minimum), min(int(maximum), number))


def _coerce_max_bytes(value: Any, *, default: int = 32768, maximum: int = 262144) -> int:
    try:
        number = int(value)
    except Exception:
        number = int(default)
    return max(1, min(int(maximum), number))


def _tail_lines(path: Path, *, limit: int) -> List[str]:
    """Return the last ``limit`` lines without assuming the file is small."""

    if limit <= 0:
        return []
    # The operator log is normally small, but avoid unbounded memory use.
    block_size = 8192
    chunks: List[bytes] = []
    newline_count = 0
    with path.open("rb") as fh:
        fh.seek(0, os.SEEK_END)
        pos = fh.tell()
        while pos > 0 and newline_count <= limit:
            read_size = min(block_size, pos)
            pos -= read_size
            fh.seek(pos)
            chunk = fh.read(read_size)
            chunks.append(chunk)
            newline_count += chunk.count(b"\n")
    data = b"".join(reversed(chunks))
    text = data.decode("utf-8", errors="replace")
    return text.splitlines()[-limit:]


def read_operator_log(path: Optional[Path], *, limit: Any = 100) -> JsonDict:
    """Read the tail of ``operator_console.jsonl`` as parsed JSON entries."""

    n = _coerce_limit(limit, default=100, maximum=1000)
    if path is None:
        return {
            "ok": False,
            "reason": "operator log path is not configured",
            "path": None,
            "entries": [],
        }
    log_path = Path(path)
    if not log_path.exists():
        return {
            "ok": True,
            "reason": "operator log file does not exist yet",
            "path": str(log_path),
            "entries": [],
            "line_count": 0,
        }
    if not log_path.is_file():
        return {
            "ok": False,
            "reason": "operator log path is not a regular file",
            "path": str(log_path),
            "entries": [],
        }
    entries: List[JsonDict] = []
    parse_errors: List[JsonDict] = []
    lines = _tail_lines(log_path, limit=n)
    start_line = None
    try:
        # Only used for display context; bounded file-tail parsing remains above.
        with log_path.open("rb") as fh:
            total_lines = sum(1 for _ in fh)
        start_line = max(1, total_lines - len(lines) + 1)
    except Exception:
        total_lines = None
    for idx, line in enumerate(lines, start=start_line or 1):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
            if isinstance(payload, dict):
                payload.setdefault("_line", idx)
                entries.append(payload)
            else:
                parse_errors.append({"line": idx, "reason": "JSON value is not an object"})
        except Exception as exc:
            parse_errors.append({"line": idx, "reason": str(exc), "text": line[:200]})
    return {
        "ok": not bool(parse_errors),
        "reason": "operator log read" if not parse_errors else "operator log read with parse warnings",
        "path": str(log_path),
        "entries": entries,
        "parse_errors": parse_errors,
        "requested_limit": n,
        "returned_count": len(entries),
        "line_count": total_lines,
    }


def _safe_relative_to(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def _allowed_log_path(
    requested: Any,
    *,
    launcher_log_dir: Optional[Path],
    operator_log_path: Optional[Path],
    extra_roots: Optional[Iterable[Path]] = None,
) -> Tuple[Optional[Path], Optional[str]]:
    if requested in (None, ""):
        return None, "log path is empty"
    candidate = Path(str(requested)).expanduser()
    if not candidate.is_absolute():
        return None, "log path must be absolute"
    roots: List[Path] = []
    if launcher_log_dir is not None:
        roots.append(Path(launcher_log_dir))
    if operator_log_path is not None:
        roots.append(Path(operator_log_path).parent)
    if extra_roots is not None:
        roots.extend(Path(root) for root in extra_roots)
    candidate_resolved = candidate.resolve()
    for root in roots:
        if _safe_relative_to(candidate_resolved, Path(root)):
            return candidate_resolved, None
    return None, "requested log path is outside configured console log directories"


def read_text_log(
    requested_path: Any,
    *,
    launcher_log_dir: Optional[Path],
    operator_log_path: Optional[Path],
    max_bytes: Any = 32768,
    extra_roots: Optional[Iterable[Path]] = None,
) -> JsonDict:
    """Read the tail of an allowed local text log file."""

    path, error = _allowed_log_path(
        requested_path,
        launcher_log_dir=launcher_log_dir,
        operator_log_path=operator_log_path,
        extra_roots=extra_roots,
    )
    if error is not None or path is None:
        return {"ok": False, "reason": error or "invalid log path", "path": str(requested_path or "")}
    if not path.exists():
        return {"ok": False, "reason": "log file does not exist", "path": str(path)}
    if not path.is_file():
        return {"ok": False, "reason": "log path is not a regular file", "path": str(path)}
    size = path.stat().st_size
    nbytes = _coerce_max_bytes(max_bytes)
    with path.open("rb") as fh:
        if size > nbytes:
            fh.seek(size - nbytes)
            raw = fh.read(nbytes)
            truncated = True
        else:
            raw = fh.read()
            truncated = False
    return {
        "ok": True,
        "reason": "log file read",
        "path": str(path),
        "size_bytes": size,
        "returned_bytes": len(raw),
        "truncated_head": truncated,
        "text": raw.decode("utf-8", errors="replace"),
    }


def launcher_log_choices(processes: Iterable[Mapping[str, Any]]) -> List[JsonDict]:
    """Return stdout/stderr paths from process records for UI display."""

    out: List[JsonDict] = []
    for record in processes:
        pid = record.get("pid")
        label = record.get("label") or record.get("action") or "launcher"
        for stream, key in (("stdout", "stdout_path"), ("stderr", "stderr_path")):
            path = record.get(key)
            if path:
                out.append({
                    "pid": pid,
                    "label": label,
                    "stream": stream,
                    "path": path,
                    "status": record.get("status"),
                })
    return out
