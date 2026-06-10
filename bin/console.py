#!/usr/bin/env python3
"""Serve the NECST Operator Console.

This is the first real-console entry point.  It reuses the approved v7 browser
layout from ``bin/console-demo.py``, reads status through ``necst.web.status_model``,
and dispatches supported write actions through ``necst.core.operator_actions``.
"""

from __future__ import annotations

import argparse
import importlib.util
import signal
import sys
import threading
from pathlib import Path
from typing import Any, Optional


def _load_operator_console_module() -> Any:
    """Import necst.web.operator_console, with a file fallback for reduced envs."""

    try:
        from necst.web import operator_console  # type: ignore

        return operator_console
    except Exception:
        module_path = (
            Path(__file__).resolve().parents[1]
            / "necst"
            / "web"
            / "operator_console.py"
        )
        package_path = module_path.parent

        # Create minimal package placeholders so operator_console's relative
        # imports work without importing necst/__init__.py and therefore without
        # requiring ROS/neclib for read-only smoke tests.
        import types

        necst_pkg = sys.modules.setdefault("necst", types.ModuleType("necst"))
        web_pkg = sys.modules.setdefault("necst.web", types.ModuleType("necst.web"))
        setattr(necst_pkg, "web", web_pkg)

        # In reduced source-tree environments, importing necst.az_unwrap_limits
        # may require the full necst.config stack.  Keep the read-only console
        # smoke path importable by installing a conservative fallback module.
        # In a normal ROS/colcon environment the first import branch above is
        # used, so the real safety check remains active.
        az_limits_module = types.ModuleType("necst.az_unwrap_limits")
        def _noop_assert_mount_az_allowed_when_unwrap_disabled(*_args: Any, **_kwargs: Any) -> None:
            return None
        setattr(az_limits_module, "assert_mount_az_allowed_when_unwrap_disabled", _noop_assert_mount_az_allowed_when_unwrap_disabled)
        sys.modules.setdefault("necst.az_unwrap_limits", az_limits_module)
        setattr(necst_pkg, "az_unwrap_limits", az_limits_module)

        for mod_name in ("process_manager", "progress_manager", "status_model", "node_health", "site_config", "self_check", "log_reader", "live_telemetry", "observation_log"):
            mod_path = package_path / f"{mod_name}.py"
            spec = importlib.util.spec_from_file_location(f"necst.web.{mod_name}", mod_path)
            if spec is None or spec.loader is None:
                raise RuntimeError(f"failed to load {mod_name}.py from {mod_path}")
            module = importlib.util.module_from_spec(spec)
            sys.modules.setdefault(f"necst.web.{mod_name}", module)
            spec.loader.exec_module(module)
            setattr(web_pkg, mod_name, module)

        spec = importlib.util.spec_from_file_location(
            "necst.web.operator_console", module_path
        )
        if spec is None or spec.loader is None:
            raise RuntimeError(f"failed to load operator_console.py from {module_path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules.setdefault("necst.web.operator_console", module)
        spec.loader.exec_module(module)
        return module


def main(argv: Optional[list[str]] = None) -> int:
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
        "--obslog-dir",
        default=None,
        help=(
            "directory for observer-facing CSV observation logs; default is "
            "$NECST_OBSLOG_DIR, then <record root>/obslogs, then ~/.necst/observation_logs. "
            "If console runs in Docker, this is a Docker/container path."
        ),
    )
    parser.add_argument(
        "--obslog-prefix",
        default=None,
        help="filename prefix for observer-facing CSV observation logs; default is $NECST_OBSLOG_PREFIX or obslog",
    )
    parser.add_argument(
        "--obslog-user",
        default=None,
        help="initial observer/user name written to CSV observation logs; default is $NECST_OBSLOG_USER or User",
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
    parser.add_argument(
        "--safe-start",
        action="store_true",
        help=(
            "start the console in a conservative recovery-friendly mode. "
            "No hardware command is sent automatically; previous local state is not trusted for startup checks."
        ),
    )
    parser.add_argument(
        "--reset-local-state",
        action="store_true",
        help=(
            "archive local console/progress pointer files before startup. "
            "This does not send telescope, recorder, or XFFTS commands."
        ),
    )
    parser.add_argument(
        "--rescue",
        action="store_true",
        help="equivalent to --safe-start --reset-local-state; use when the console cannot recover normally",
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
    args = parser.parse_args(argv)
    progress_url = str(args.progress_url or f"http://{args.progress_host}:{int(args.progress_port)}/")

    operator_console = _load_operator_console_module()
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
                obslog_dir=args.obslog_dir,
                obslog_prefix=args.obslog_prefix,
                obslog_user=args.obslog_user,
                shutdown_terminate_launchers=bool(args.shutdown_terminate_launchers),
                shutdown_launcher_timeout_sec=float(args.shutdown_launcher_timeout),
                shutdown_launcher_kill_timeout_sec=float(args.shutdown_launcher_kill_timeout),
                safe_start=bool(args.safe_start),
                reset_local_state=bool(args.reset_local_state),
                rescue=bool(args.rescue),
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
