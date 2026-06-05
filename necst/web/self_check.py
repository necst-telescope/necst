"""Operator-console self-check helpers.

The console needs a cheap, read-only preflight before observers trust it for
live operation.  This module checks the console's own wiring and configuration;
it does not send telescope commands, does not start launchers, and does not
start or stop the progress monitor.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from . import status_model

JsonDict = Dict[str, Any]

_REQUIRED_MOUNT_LIMITS = ("az_min", "az_max", "el_min", "el_max")
_EXPECTED_CAPABILITIES = (
    "mount_move",
    "target_tracking",
    "observation_start",
    "rsky",
    "skydip",
    "chopper_status",
    "chopper_move",
    "chopper_maintenance",
)


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())


def _check_item(
    name: str,
    ok: bool,
    message: str,
    *,
    severity: str = "error",
    data: Optional[Mapping[str, Any]] = None,
) -> JsonDict:
    return {
        "name": str(name),
        "ok": bool(ok),
        "severity": str(severity),
        "message": str(message),
        "data": dict(data or {}),
    }


def _is_writable_dir(path: Path) -> bool:
    """Return whether *path* is an existing writable directory.

    This intentionally avoids creating files so /api/self-check remains a
    read-only diagnostic endpoint.
    """

    try:
        return path.is_dir() and os.access(str(path), os.W_OK | os.X_OK)
    except Exception:
        return False


def _mount_limit_check(mount_limits: Mapping[str, Any]) -> JsonDict:
    missing = [key for key in _REQUIRED_MOUNT_LIMITS if key not in mount_limits]
    if missing:
        return _check_item(
            "mount_limits",
            False,
            "site TOML mount limits are incomplete",
            data={"missing": missing, "mount_limits": dict(mount_limits)},
        )
    try:
        az_min = float(mount_limits["az_min"])
        az_max = float(mount_limits["az_max"])
        el_min = float(mount_limits["el_min"])
        el_max = float(mount_limits["el_max"])
    except Exception as exc:
        return _check_item(
            "mount_limits",
            False,
            f"site TOML mount limits are not numeric: {exc}",
            data={"mount_limits": dict(mount_limits)},
        )
    if az_min >= az_max or el_min >= el_max:
        return _check_item(
            "mount_limits",
            False,
            "site TOML mount min/max order is invalid",
            data={"mount_limits": dict(mount_limits)},
        )
    return _check_item(
        "mount_limits",
        True,
        f"mount limits OK: Az {az_min:g}..{az_max:g} deg, El {el_min:g}..{el_max:g} deg",
        severity="info",
        data={"mount_limits": dict(mount_limits)},
    )


def _capability_check(capabilities: Mapping[str, Any]) -> JsonDict:
    missing = [key for key in _EXPECTED_CAPABILITIES if key not in capabilities]
    non_bool = [key for key, value in capabilities.items() if not isinstance(value, bool)]
    if missing or non_bool:
        return _check_item(
            "site_capabilities",
            False,
            "site capabilities are incomplete or non-boolean",
            data={"missing": missing, "non_bool": non_bool, "capabilities": dict(capabilities)},
        )
    disabled = sorted(key for key, value in capabilities.items() if not value)
    message = "site capabilities OK"
    severity = "info"
    if disabled:
        message += "; disabled: " + ", ".join(disabled)
        severity = "warning"
    return _check_item(
        "site_capabilities",
        True,
        message,
        severity=severity,
        data={"disabled": disabled, "capabilities": dict(capabilities)},
    )


def _chopper_check(chopper: Mapping[str, Any], capabilities: Mapping[str, Any]) -> JsonDict:
    if not bool(capabilities.get("chopper_move", False)):
        return _check_item(
            "chopper_config",
            True,
            "chopper move is disabled by site capability",
            severity="info",
            data={"chopper": dict(chopper)},
        )
    missing = [
        key
        for key in ("insert_position", "remove_position")
        if chopper.get(key) in (None, "")
    ]
    if missing:
        return _check_item(
            "chopper_config",
            False,
            "chopper move is enabled but IN/OUT positions are missing",
            data={"missing": missing, "chopper": dict(chopper)},
        )
    return _check_item(
        "chopper_config",
        True,
        f"chopper endpoints OK: IN={chopper.get('insert_position')}, OUT={chopper.get('remove_position')}",
        severity="info",
        data={"chopper": dict(chopper)},
    )


def _log_path_check(path_value: Any, *, name: str, expect_file: bool = False) -> JsonDict:
    if path_value in (None, ""):
        return _check_item(name, False, f"{name} is not configured")
    path = Path(str(path_value))
    directory = path.parent if expect_file else path
    ok = _is_writable_dir(directory)
    if not ok:
        return _check_item(
            name,
            False,
            f"{directory} is not an existing writable directory",
            data={"path": str(path), "directory": str(directory)},
        )
    return _check_item(
        name,
        True,
        f"{directory} is writable",
        severity="info",
        data={"path": str(path), "directory": str(directory)},
    )


def run_console_self_check(
    *,
    telescope: str,
    action_mode: str,
    live_actions_enabled: bool,
    progress_root: Path,
    site_summary: Any,
    mount_limits: Mapping[str, Any],
    capabilities: Mapping[str, Any],
    chopper_config: Mapping[str, Any],
    operator_log_path: Optional[Path],
    launcher_log_dir: Optional[Path],
    process_registry: Any,
    progress_monitor: Any,
    events_limit: int = 12,
    include_progress_health: bool = True,
) -> JsonDict:
    """Return a JSON-serialisable read-only console self-check result."""

    checks: List[JsonDict] = []
    checks.append(
        _check_item(
            "action_mode",
            action_mode in {"live", "dry-run"},
            f"action_mode={action_mode}",
            severity="info" if action_mode in {"live", "dry-run"} else "error",
        )
    )
    if action_mode == "live" and not live_actions_enabled:
        checks.append(
            _check_item(
                "live_write_guard",
                True,
                "live write actions are guarded by --guard-live-actions; STOP/ABORT remain available",
                severity="warning",
                data={
                    "live_actions_enabled": False,
                    "guard_option": "--guard-live-actions",
                },
            )
        )
    elif action_mode == "live" and live_actions_enabled:
        checks.append(
            _check_item(
                "live_write_guard",
                True,
                "live write actions are enabled by default in live mode",
                severity="info",
                data={"live_actions_enabled": True},
            )
        )
    else:
        checks.append(
            _check_item(
                "live_write_guard",
                True,
                "dry-run mode: no live command is sent",
                severity="info",
                data={"live_actions_enabled": False},
            )
        )

    site_dict = site_summary.to_dict() if hasattr(site_summary, "to_dict") else {}
    site_source = site_dict.get("source") or "unknown"
    site_warnings = list(site_dict.get("warnings") or [])
    checks.append(
        _check_item(
            "site_config",
            not bool(site_warnings) and site_source != "unresolved",
            f"site config source={site_source}, observatory={site_dict.get('observatory') or 'unknown'}",
            severity="warning" if site_warnings else "info",
            data={"site": site_dict, "warnings": site_warnings},
        )
    )
    checks.append(_mount_limit_check(mount_limits))
    checks.append(_capability_check(capabilities))
    checks.append(_chopper_check(chopper_config, capabilities))

    progress_root = Path(progress_root)
    checks.append(
        _check_item(
            "progress_root",
            progress_root.exists() and progress_root.is_dir(),
            f"progress root {'exists' if progress_root.exists() else 'does not exist'}: {progress_root}",
            severity="info" if progress_root.exists() and progress_root.is_dir() else "warning",
            data={"path": str(progress_root)},
        )
    )
    checks.append(_log_path_check(operator_log_path, name="operator_log", expect_file=True))
    checks.append(_log_path_check(launcher_log_dir, name="launcher_log_dir", expect_file=False))

    try:
        counts = process_registry.counts()
        active_count = int(counts.get("active", 0))
        checks.append(
            _check_item(
                "launcher_registry",
                True,
                f"launcher registry OK; active={active_count}, total={counts.get('total', 0)}",
                severity="warning" if active_count else "info",
                data={"process_counts": counts},
            )
        )
    except Exception as exc:
        checks.append(_check_item("launcher_registry", False, f"launcher registry failed: {exc}"))

    progress_data: JsonDict = {"url": None, "running": False, "owned_by_console": False}
    if progress_monitor is None:
        checks.append(
            _check_item(
                "progress_monitor",
                True,
                "progress monitor manager is not configured",
                severity="warning",
                data=progress_data,
            )
        )
    else:
        try:
            status = progress_monitor.status(check_external=bool(include_progress_health))
            progress_data = status.to_dict()
            checks.append(
                _check_item(
                    "progress_monitor",
                    True,
                    str(progress_data.get("message") or progress_data.get("status") or "progress monitor status OK"),
                    severity="info" if progress_data.get("running") else "warning",
                    data=progress_data,
                )
            )
        except Exception as exc:
            checks.append(_check_item("progress_monitor", False, f"progress monitor status failed: {exc}"))

    try:
        status_state = status_model.build_progress_status_state(
            progress_root,
            events_limit=int(events_limit),
        )
        op = status_state.get("operator_status", {})
        checks.append(
            _check_item(
                "status_model",
                isinstance(op, Mapping),
                "status_model.build_progress_status_state returned operator_status"
                if isinstance(op, Mapping)
                else "status_model did not return operator_status mapping",
                severity="info" if isinstance(op, Mapping) else "error",
                data={"operator_status_schema": op.get("schema") if isinstance(op, Mapping) else None},
            )
        )
    except Exception as exc:
        checks.append(_check_item("status_model", False, f"status model failed: {exc}"))

    error_count = sum(
        1 for item in checks if not item.get("ok") and item.get("severity") == "error"
    )
    warning_count = sum(
        1
        for item in checks
        if item.get("severity") == "warning" or (not item.get("ok") and item.get("severity") != "error")
    )
    ok = error_count == 0
    status = "ok" if ok and warning_count == 0 else ("warning" if ok else "error")
    summary = {
        "ok": ok,
        "status": status,
        "error_count": error_count,
        "warning_count": warning_count,
        "check_count": len(checks),
        "telescope": str(telescope),
        "action_mode": str(action_mode),
        "live_actions_enabled": bool(live_actions_enabled),
        "site_source": site_source,
        "observatory": site_dict.get("observatory"),
        "checked_at_iso": _now_iso(),
    }
    return {
        "ok": ok,
        "status": status,
        "summary": summary,
        "checks": checks,
    }
