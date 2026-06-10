"""Small, safe log readers for the NECST operator console."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional


def _tail_lines(path: Path, *, limit: int = 100) -> List[str]:
    limit = max(1, min(int(limit), 5000))
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
        return [line.rstrip("\n") for line in lines[-limit:]]
    except Exception:
        return []


def read_operator_log(path_value: Any, *, limit: Any = 100) -> Dict[str, Any]:
    if path_value in (None, ""):
        return {"ok": False, "reason": "operator log path is not configured", "entries": []}
    path = Path(str(path_value)).expanduser()
    if not path.exists():
        return {"ok": True, "reason": "operator log does not exist yet", "path": str(path), "entries": []}
    entries: List[Dict[str, Any]] = []
    parse_errors = 0
    for line in _tail_lines(path, limit=int(limit or 100)):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
            if isinstance(payload, dict):
                entries.append(payload)
            else:
                entries.append({"message": str(payload)})
        except Exception:
            parse_errors += 1
            entries.append({"message": line, "parse_error": True})
    return {
        "ok": True,
        "reason": f"read {len(entries)} operator log entrie(s)",
        "path": str(path),
        "entries": entries,
        "parse_errors": parse_errors,
    }


def _allowed_paths(
    *,
    launcher_log_dir: Optional[Path],
    operator_log_path: Optional[Path],
    extra_roots: Optional[Iterable[Path]] = None,
) -> List[Path]:
    roots: List[Path] = []
    for item in (launcher_log_dir,):
        if item is not None:
            roots.append(Path(item).expanduser())
    if operator_log_path is not None:
        roots.append(Path(operator_log_path).expanduser().parent)
    for item in extra_roots or []:
        if item is not None:
            roots.append(Path(item).expanduser())
    out: List[Path] = []
    seen = set()
    for root in roots:
        try:
            key = str(root.resolve())
        except Exception:
            key = str(root)
        if key not in seen:
            seen.add(key)
            out.append(root)
    return out


def _resolve_safe_log_path(
    path_value: Any,
    *,
    launcher_log_dir: Optional[Path],
    operator_log_path: Optional[Path],
    extra_roots: Optional[Iterable[Path]] = None,
) -> Path:
    raw = str(path_value or "").strip()
    if not raw:
        raise ValueError("log file path is empty")
    path = Path(raw).expanduser()
    roots = _allowed_paths(launcher_log_dir=launcher_log_dir, operator_log_path=operator_log_path, extra_roots=extra_roots)
    if not path.is_absolute():
        if any(part == ".." for part in path.parts):
            raise ValueError("relative log path must not contain '..'")
        if launcher_log_dir is None:
            raise ValueError("relative log path requires launcher_log_dir")
        path = Path(launcher_log_dir).expanduser() / path
    resolved = path.resolve()
    for root in roots:
        try:
            resolved.relative_to(root.resolve())
            return resolved
        except Exception:
            continue
    raise ValueError(f"log path is outside allowed log directories: {path}")


def read_text_log(
    path_value: Any,
    *,
    launcher_log_dir: Optional[Path],
    operator_log_path: Optional[Path],
    max_bytes: Any = 32768,
    extra_roots: Optional[Iterable[Path]] = None,
) -> Dict[str, Any]:
    try:
        path = _resolve_safe_log_path(
            path_value,
            launcher_log_dir=launcher_log_dir,
            operator_log_path=operator_log_path,
            extra_roots=extra_roots,
        )
        max_b = max(1024, min(int(max_bytes or 32768), 1024 * 1024))
        if not path.exists() or not path.is_file():
            return {"ok": False, "reason": f"log file does not exist: {path}", "path": str(path)}
        size = path.stat().st_size
        with path.open("rb") as fh:
            if size > max_b:
                fh.seek(size - max_b)
            data = fh.read(max_b)
        return {
            "ok": True,
            "reason": "log file read",
            "path": str(path),
            "text": data.decode("utf-8", errors="replace"),
            "size_bytes": size,
            "returned_bytes": len(data),
            "truncated_head": size > max_b,
        }
    except Exception as exc:
        return {"ok": False, "reason": str(exc), "path": str(path_value or "")}


def launcher_log_choices(process_records: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    choices: List[Dict[str, Any]] = []
    for record in process_records or []:
        if not isinstance(record, Mapping):
            continue
        label = str(record.get("label") or record.get("category") or "launcher")
        pid = record.get("pid")
        for stream, key in (("stdout", "stdout_path"), ("stderr", "stderr_path")):
            path = record.get(key)
            if not path:
                continue
            choices.append({
                "label": f"{label} pid={pid} {stream}",
                "path": str(path),
                "stream": stream,
                "pid": pid,
                "category": record.get("category"),
            })
    return choices
