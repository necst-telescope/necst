"""Installable entry point for the NECST Operator Console.

This module mirrors ``bin/console.py`` so the GUI can be started after a normal
ROS/colcon installation without relying on the source-tree ``bin`` directory.
"""

from __future__ import annotations

import argparse
import signal
import threading
from pathlib import Path
from typing import Any, Optional

from . import operator_console


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve the NECST Operator Console")
    parser.add_argument("--host", default="127.0.0.1", help="web bind address")
    parser.add_argument("--port", type=int, default=8092, help="web bind port")
    parser.add_argument("--telescope", default="NECST", help="telescope label shown in the UI")
    parser.add_argument(
        "--progress-root",
        default="/tmp/necst_progress",
        help="directory containing progress sidecar files",
    )
    parser.add_argument(
        "--progress-url",
        default=None,
        help="URL opened by Launch progress monitor; default is http://<progress-host>:<progress-port>/",
    )
    parser.add_argument(
        "--progress-host",
        default="127.0.0.1",
        help="bind address for a progress.py server launched by this console",
    )
    parser.add_argument(
        "--progress-port",
        type=int,
        default=8091,
        help="port for a progress.py server launched by this console",
    )
    parser.add_argument(
        "--progress-refresh-ms",
        type=int,
        default=500,
        help="browser refresh interval [ms] for a progress.py server launched by this console",
    )
    parser.add_argument(
        "--no-ros",
        action="store_true",
        help="disable ROS subscriptions for the console status panel; normally not used on the telescope",
    )
    parser.add_argument(
        "--status-refresh-ms",
        type=int,
        default=1000,
        help="browser refresh interval [ms] for this operator console status panel",
    )
    parser.add_argument(
        "--progress-no-ros",
        action="store_true",
        help="launch progress.py with --no-ros when started from this console",
    )
    parser.add_argument(
        "--progress-log-dir",
        default=None,
        help="directory for progress.py stdout/stderr when launched by this console; default is <operator-log-dir>/progress_logs",
    )
    parser.add_argument(
        "--operator-log-dir",
        default=None,
        help="directory for persistent operator JSONL log; default is $NECST_OPERATOR_LOG_DIR or ~/.necst/operator_console",
    )
    parser.add_argument(
        "--launcher-log-dir",
        default=None,
        help="directory for launcher stdout/stderr logs; default is <operator-log-dir>/launcher_logs",
    )
    parser.add_argument(
        "--obs-root",
        action="append",
        default=None,
        help=(
            "NECST-side file chooser start location; may be repeated. "
            "Usually optional: without this, Home, common obs/data directories, and / are offered. "
            "If console runs in Docker, this is a Docker/container path. "
            "Use this to restrict or add chooser locations; also configurable with NECST_CONSOLE_OBS_ROOTS."
        ),
    )
    parser.add_argument(
        "--shutdown-terminate-launchers",
        dest="shutdown_terminate_launchers",
        action="store_true",
        default=True,
        help="terminate local launcher subprocesses on console shutdown (default)",
    )
    parser.add_argument(
        "--no-shutdown-terminate-launchers",
        dest="shutdown_terminate_launchers",
        action="store_false",
        help="leave local launcher subprocesses running when the console exits",
    )
    parser.add_argument(
        "--shutdown-launcher-timeout",
        type=float,
        default=3.0,
        help="seconds to wait after SIGTERM before escalating launcher cleanup",
    )
    parser.add_argument(
        "--shutdown-launcher-kill-timeout",
        type=float,
        default=1.0,
        help="seconds to wait after SIGKILL during launcher cleanup",
    )
    parser.add_argument(
        "--action-mode",
        choices=["live", "dry-run"],
        default="live",
        help="live may send supported actions; dry-run validates without sending commands",
    )
    parser.add_argument(
        "--guard-live-actions",
        action="store_true",
        help=(
            "block write-like actions even in live mode; normally not used because "
            "--action-mode dry-run is the recommended no-hardware mode"
        ),
    )
    parser.add_argument("--open", action="store_true", help="open the URL in a browser")
    parser.add_argument("--quiet", action="store_true", help="suppress HTTP request logs")
    parser.add_argument("--events-limit", type=int, default=12, help="number of recent events in status model")
    parser.add_argument(
        "--site-config",
        default=None,
        help="explicit site TOML path for console validation; defaults to active necst.config",
    )
    parser.add_argument("--az-min", type=float, default=None, help="fallback mount Az lower limit [deg]")
    parser.add_argument("--az-max", type=float, default=None, help="fallback mount Az upper limit [deg]")
    parser.add_argument("--el-min", type=float, default=None, help="fallback mount El lower limit [deg]")
    parser.add_argument("--el-max", type=float, default=None, help="fallback mount El upper limit [deg]")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    progress_url = str(args.progress_url or f"http://{args.progress_host}:{int(args.progress_port)}/")
    stop_event = threading.Event()

    def _signal_handler(_signum: int, _frame: Any) -> None:
        stop_event.set()

    old_int = signal.signal(signal.SIGINT, _signal_handler)
    old_term = signal.signal(signal.SIGTERM, _signal_handler)
    try:
        return int(
            operator_console.run_server(
                host=str(args.host),
                port=int(args.port),
                telescope=str(args.telescope),
                progress_root=Path(args.progress_root),
                progress_url=progress_url,
                progress_host=str(args.progress_host),
                progress_port=int(args.progress_port),
                progress_refresh_ms=int(args.progress_refresh_ms),
                status_refresh_ms=int(args.status_refresh_ms),
                progress_no_ros=bool(args.progress_no_ros),
                progress_log_dir=args.progress_log_dir,
                obs_roots=args.obs_root,
                status_no_ros=bool(args.no_ros),
                action_mode=str(args.action_mode),
                live_actions_enabled=(str(args.action_mode) == "live" and not bool(args.guard_live_actions)),
                quiet=bool(args.quiet),
                open_browser=bool(args.open),
                events_limit=int(args.events_limit),
                site_config_path=args.site_config,
                operator_log_dir=args.operator_log_dir,
                launcher_log_dir=args.launcher_log_dir,
                shutdown_terminate_launchers=bool(args.shutdown_terminate_launchers),
                shutdown_launcher_timeout_sec=float(args.shutdown_launcher_timeout),
                shutdown_launcher_kill_timeout_sec=float(args.shutdown_launcher_kill_timeout),
                az_min=args.az_min,
                az_max=args.az_max,
                el_min=args.el_min,
                el_max=args.el_max,
                stop_event=stop_event,
            )
        )
    finally:
        signal.signal(signal.SIGINT, old_int)
        signal.signal(signal.SIGTERM, old_term)


if __name__ == "__main__":
    raise SystemExit(main())
