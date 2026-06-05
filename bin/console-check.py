#!/usr/bin/env python3
"""Run a smoke check against an already running NECST Operator Console."""

from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any, Optional


def _load_console_smoke_module() -> Any:
    try:
        from necst.web import console_smoke  # type: ignore

        return console_smoke
    except Exception:
        module_path = Path(__file__).resolve().parents[1] / "necst" / "web" / "console_smoke.py"
        spec = importlib.util.spec_from_file_location("necst.web.console_smoke", module_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"failed to load console_smoke.py from {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules.setdefault("necst.web.console_smoke", module)
        spec.loader.exec_module(module)
        return module


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only smoke check for a running NECST Operator Console"
    )
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8092/",
        help="operator console base URL",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=3.0,
        help="per-request timeout [s]",
    )
    parser.add_argument(
        "--skip-action-self-check",
        action="store_true",
        help="do not POST action=self_check to /api/action",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="exit non-zero when warnings are present",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="print full JSON result instead of a compact report",
    )
    args = parser.parse_args(argv)

    console_smoke = _load_console_smoke_module()
    result = console_smoke.run_smoke_test(
        url=str(args.url),
        timeout=float(args.timeout),
        include_action_self_check=not bool(args.skip_action_self_check),
        fail_on_warning=bool(args.fail_on_warning),
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        console_smoke.print_smoke_report(result)
    return 0 if bool(result.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
