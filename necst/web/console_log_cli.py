"""CLI helpers for reading the persistent NECST Operator Console log."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any, Optional

from . import log_reader


def default_operator_log_dir() -> Path:
    override = os.environ.get("NECST_OPERATOR_LOG_DIR")
    if override:
        return Path(override).expanduser()
    return Path.home() / ".necst" / "operator_console"


def default_operator_log_path(log_dir: Optional[str | os.PathLike[str]] = None) -> Path:
    base = Path(log_dir).expanduser() if log_dir not in (None, "") else default_operator_log_dir()
    return base / "operator_console.jsonl"


def _print_text_tail(payload: dict[str, Any]) -> int:
    path = payload.get("path")
    if path:
        print(f"operator log: {path}")
    if payload.get("ok") is False:
        print(payload.get("reason") or "operator log read failed")
        return 1
    entries = payload.get("entries") or []
    if not entries:
        print(payload.get("reason") or "operator log is empty")
        return 0
    for entry in entries:
        when = entry.get("time_iso") or entry.get("time") or ""
        mark = "NG" if entry.get("ok") is False else "OK"
        action = entry.get("action") or ""
        message = entry.get("message") or entry.get("reason") or ""
        print(f"{when} {mark} {action}: {message}".rstrip())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read NECST Operator Console logs")
    parser.add_argument(
        "command",
        nargs="?",
        default="tail",
        choices=["tail", "path", "cat"],
        help="tail parsed JSONL entries, print path, or print raw JSONL tail",
    )
    parser.add_argument("--operator-log-dir", default=None, help="operator log directory; default is $NECST_OPERATOR_LOG_DIR or ~/.necst/operator_console")
    parser.add_argument("--path", default=None, help="explicit operator_console.jsonl path")
    parser.add_argument("--limit", type=int, default=80, help="number of JSONL records/lines to show")
    parser.add_argument("--json", action="store_true", help="print parsed payload as JSON")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    log_path = Path(args.path).expanduser() if args.path else default_operator_log_path(args.operator_log_dir)
    if args.command == "path":
        print(str(log_path))
        return 0
    payload = log_reader.read_operator_log(log_path, limit=args.limit)
    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if payload.get("ok") is not False else 1
    if args.command == "cat":
        if not log_path.exists():
            print(f"operator log does not exist yet: {log_path}")
            return 0
        text_payload = log_reader.read_text_log(
            str(log_path),
            launcher_log_dir=None,
            operator_log_path=log_path,
            max_bytes=262144,
        )
        if text_payload.get("ok") is False:
            print(text_payload.get("reason") or "log read failed")
            return 1
        print(text_payload.get("text") or "", end="")
        return 0
    return _print_text_tail(payload)


if __name__ == "__main__":
    raise SystemExit(main())
