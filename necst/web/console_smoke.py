"""HTTP smoke-test helpers for the NECST Operator Console.

The smoke test is intentionally read-only by default.  It talks to an already
running console server and verifies that the operator-facing status, process,
and self-check endpoints are wired together.  It does not move the telescope,
does not move the chopper, and does not start observation launchers.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

JsonDict = Dict[str, Any]


@dataclass
class SmokeItem:
    """One smoke-test item."""

    name: str
    ok: bool
    message: str
    severity: str = "error"
    data: JsonDict = field(default_factory=dict)

    def to_dict(self) -> JsonDict:
        return {
            "name": self.name,
            "ok": bool(self.ok),
            "severity": self.severity,
            "message": self.message,
            "data": dict(self.data),
        }


def _normalise_base_url(url: str) -> str:
    url = str(url or "").strip()
    if not url:
        raise ValueError("console URL is empty")
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    return url.rstrip("/") + "/"


def _endpoint_url(base_url: str, path: str) -> str:
    return urllib.parse.urljoin(base_url, path.lstrip("/"))


def _json_request(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    payload: Optional[Mapping[str, Any]] = None,
    timeout: float = 3.0,
) -> JsonDict:
    url = _endpoint_url(base_url, path)
    data: Optional[bytes] = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json; charset=utf-8"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=float(timeout)) as response:
        body = response.read().decode("utf-8")
        if int(response.status) < 200 or int(response.status) >= 300:
            raise RuntimeError(f"{method} {path} returned HTTP {response.status}")
    try:
        parsed = json.loads(body or "{}")
    except Exception as exc:
        raise RuntimeError(f"{method} {path} did not return JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(f"{method} {path} returned non-object JSON")
    return parsed


def _item(
    name: str,
    ok: bool,
    message: str,
    *,
    severity: str = "error",
    data: Optional[Mapping[str, Any]] = None,
) -> SmokeItem:
    return SmokeItem(name=name, ok=bool(ok), message=str(message), severity=str(severity), data=dict(data or {}))


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> List[Any]:
    return list(value) if isinstance(value, list) else []


def _check_health(base_url: str, timeout: float) -> Tuple[SmokeItem, JsonDict]:
    try:
        data = _json_request(base_url, "/health", timeout=timeout)
    except Exception as exc:
        return _item("health", False, f"/health failed: {exc}"), {}
    ok = bool(data.get("ok"))
    return _item("health", ok, "/health OK" if ok else "/health did not report ok", data=data), data


def _check_status(base_url: str, timeout: float) -> Tuple[List[SmokeItem], JsonDict]:
    try:
        data = _json_request(base_url, "/api/status", timeout=timeout)
    except Exception as exc:
        return [_item("api_status", False, f"/api/status failed: {exc}")], {}

    items: List[SmokeItem] = [
        _item("api_status", True, "/api/status returned JSON", severity="info")
    ]
    mount_limits = _mapping(data.get("mount_limits"))
    missing_limits = [k for k in ("az_min", "az_max", "el_min", "el_max") if k not in mount_limits]
    items.append(
        _item(
            "status_mount_limits",
            not missing_limits,
            "mount limits present" if not missing_limits else "mount limits are incomplete",
            data={"missing": missing_limits, "mount_limits": dict(mount_limits)},
        )
    )
    capabilities = _mapping(data.get("capabilities"))
    required_caps = (
        "mount_move",
        "target_tracking",
        "observation_start",
        "rsky",
        "skydip",
        "chopper_status",
        "chopper_move",
        "chopper_maintenance",
    )
    missing_caps = [k for k in required_caps if k not in capabilities]
    non_bool_caps = [k for k, v in capabilities.items() if not isinstance(v, bool)]
    items.append(
        _item(
            "status_capabilities",
            not missing_caps and not non_bool_caps,
            "capabilities present" if not missing_caps and not non_bool_caps else "capabilities are incomplete or non-boolean",
            data={"missing": missing_caps, "non_bool": non_bool_caps, "capabilities": dict(capabilities)},
        )
    )
    self_check_endpoint = data.get("self_check_endpoint")
    items.append(
        _item(
            "status_self_check_endpoint",
            self_check_endpoint == "/api/self-check",
            "self-check endpoint advertised" if self_check_endpoint == "/api/self-check" else "self-check endpoint is missing",
            data={"self_check_endpoint": self_check_endpoint},
        )
    )

    try:
        status_refresh_ms = int(data.get("status_refresh_ms"))
    except Exception:
        status_refresh_ms = 0
    items.append(
        _item(
            "status_refresh_interval",
            status_refresh_ms >= 200,
            f"console status refresh interval = {status_refresh_ms} ms" if status_refresh_ms else "console status refresh interval is missing",
            severity="info" if status_refresh_ms >= 200 else "warning",
            data={"status_refresh_ms": status_refresh_ms},
        )
    )

    live_actions = _mapping(data.get("live_actions"))
    if str(data.get("action_mode")) == "live" and live_actions.get("guarded"):
        items.append(
            _item(
                "status_live_write_guard",
                True,
                "live write actions are guarded by --guard-live-actions",
                severity="warning",
                data={"live_actions": dict(live_actions)},
            )
        )
    elif str(data.get("action_mode")) == "live" and live_actions.get("enabled"):
        items.append(
            _item(
                "status_live_write_guard",
                True,
                "live write actions are enabled by default in live mode",
                severity="info",
                data={"live_actions": dict(live_actions)},
            )
        )
    else:
        items.append(
            _item(
                "status_live_write_guard",
                True,
                "dry-run mode or live writes enabled by default",
                severity="info",
                data={"live_actions": dict(live_actions)},
            )
        )

    live_telemetry = _mapping(data.get("live_telemetry"))
    live_requested = bool(live_telemetry.get("requested"))
    live_available = bool(live_telemetry.get("available"))
    if live_requested and live_available:
        live_mode = str(data.get("action_mode")) == "live"
        has_position = bool(live_telemetry.get("has_position"))
        items.append(
            _item(
                "status_live_telemetry",
                has_position or not live_mode,
                (
                    f"console live telemetry available ({live_telemetry.get('spin_mode') or 'unknown spin mode'}); "
                    + ("position sample received" if has_position else "waiting for encoder/pointing position sample")
                ),
                severity="info" if has_position else ("error" if live_mode else "warning"),
                data={"live_telemetry": dict(live_telemetry)},
            )
        )
    elif live_requested:
        live_mode = str(data.get("action_mode")) == "live"
        items.append(
            _item(
                "status_live_telemetry",
                not live_mode,
                f"console live telemetry unavailable: {live_telemetry.get('error') or 'unknown reason'}",
                severity="error" if live_mode else "warning",
                data={"live_telemetry": dict(live_telemetry)},
            )
        )
    else:
        items.append(
            _item(
                "status_live_telemetry",
                True,
                "console live telemetry disabled",
                severity="warning" if str(data.get("action_mode")) == "live" else "info",
                data={"live_telemetry": dict(live_telemetry)},
            )
        )

    process_counts = _mapping(data.get("process_counts"))
    items.append(
        _item(
            "status_process_counts",
            isinstance(process_counts, Mapping),
            "process counts present",
            severity="info",
            data={"process_counts": dict(process_counts)},
        )
    )
    return items, data


def _check_self_check(base_url: str, timeout: float) -> Tuple[List[SmokeItem], JsonDict]:
    try:
        data = _json_request(base_url, "/api/self-check", timeout=timeout)
    except Exception as exc:
        return [_item("self_check_endpoint", False, f"/api/self-check failed: {exc}")], {}

    summary = _mapping(data.get("summary"))
    error_count = int(summary.get("error_count", 0) or 0)
    warning_count = int(summary.get("warning_count", 0) or 0)
    status = str(data.get("status") or summary.get("status") or "unknown")
    items = [
        _item(
            "self_check_endpoint",
            error_count == 0,
            f"self-check status={status}, errors={error_count}, warnings={warning_count}",
            severity="warning" if error_count == 0 and warning_count else "error",
            data={"summary": dict(summary)},
        )
    ]
    checks = _list(data.get("checks"))
    for check in checks:
        if not isinstance(check, Mapping):
            continue
        ok = bool(check.get("ok"))
        severity = str(check.get("severity") or "error")
        if not ok or severity == "warning":
            items.append(
                _item(
                    "self_check." + str(check.get("name") or "unknown"),
                    ok,
                    str(check.get("message") or ""),
                    severity=severity,
                    data={"check": dict(check)},
                )
            )
    return items, data


def _check_processes(base_url: str, timeout: float) -> Tuple[List[SmokeItem], JsonDict]:
    try:
        data = _json_request(base_url, "/api/processes", timeout=timeout)
    except Exception as exc:
        return [_item("processes_endpoint", False, f"/api/processes failed: {exc}")], {}
    counts = _mapping(data.get("process_counts"))
    items = [
        _item(
            "processes_endpoint",
            bool(counts) or "processes" in data,
            "/api/processes returned process state",
            severity="info",
            data={"process_counts": dict(counts)},
        )
    ]
    return items, data


def _check_operator_log(base_url: str, timeout: float) -> Tuple[List[SmokeItem], JsonDict]:
    try:
        data = _json_request(base_url, "/api/operator-log?limit=5", timeout=timeout)
    except Exception as exc:
        return [_item("operator_log_endpoint", False, f"/api/operator-log failed: {exc}")], {}
    ok = bool(data.get("ok", True)) and isinstance(data.get("entries", []), list)
    return [
        _item(
            "operator_log_endpoint",
            ok,
            "/api/operator-log returned JSONL tail" if ok else str(data.get("reason") or "operator log endpoint returned not-ok"),
            severity="info" if ok else "error",
            data={"path": data.get("path"), "returned_count": data.get("returned_count")},
        )
    ], data


def _check_action_self_check(base_url: str, timeout: float) -> Tuple[List[SmokeItem], JsonDict]:
    payload = {"action": "self_check", "params": {}, "session_id": "console-check"}
    try:
        data = _json_request(base_url, "/api/action", method="POST", payload=payload, timeout=timeout)
    except Exception as exc:
        return [_item("action_self_check", False, f"action=self_check failed: {exc}")], {}
    ok = bool(data.get("ok"))
    action_data = _mapping(data.get("data"))
    self_data = _mapping(action_data.get("self_check"))
    summary = _mapping(self_data.get("summary"))
    error_count = int(summary.get("error_count", 0) or 0) if summary else 0
    item_ok = ok and error_count == 0
    return [
        _item(
            "action_self_check",
            item_ok,
            "action=self_check OK" if item_ok else str(data.get("reason") or "action=self_check reported errors"),
            severity="warning" if ok and not item_ok else "error",
            data={"response": data},
        )
    ], data


def run_smoke_test(
    *,
    url: str = "http://127.0.0.1:8092/",
    timeout: float = 3.0,
    include_action_self_check: bool = True,
    fail_on_warning: bool = False,
) -> JsonDict:
    """Run a read-only smoke test against an already running console server."""

    base_url = _normalise_base_url(url)
    started_at = time.time()
    items: List[SmokeItem] = []
    raw: JsonDict = {}

    health_item, raw_health = _check_health(base_url, timeout)
    items.append(health_item)
    raw["health"] = raw_health

    status_items, raw_status = _check_status(base_url, timeout)
    items.extend(status_items)
    raw["status"] = raw_status

    self_items, raw_self = _check_self_check(base_url, timeout)
    items.extend(self_items)
    raw["self_check"] = raw_self

    process_items, raw_processes = _check_processes(base_url, timeout)
    items.extend(process_items)
    raw["processes"] = raw_processes

    log_items, raw_log = _check_operator_log(base_url, timeout)
    items.extend(log_items)
    raw["operator_log"] = raw_log

    if include_action_self_check:
        action_items, raw_action = _check_action_self_check(base_url, timeout)
        items.extend(action_items)
        raw["action_self_check"] = raw_action

    error_count = sum(1 for item in items if not item.ok and item.severity == "error")
    warning_count = sum(1 for item in items if item.severity == "warning" or (not item.ok and item.severity != "error"))
    ok = error_count == 0 and (warning_count == 0 or not bool(fail_on_warning))
    status = "ok" if ok and warning_count == 0 else ("warning" if ok else "error")
    finished_at = time.time()
    return {
        "ok": ok,
        "status": status,
        "summary": {
            "ok": ok,
            "status": status,
            "url": base_url,
            "error_count": error_count,
            "warning_count": warning_count,
            "check_count": len(items),
            "duration_sec": round(finished_at - started_at, 3),
            "fail_on_warning": bool(fail_on_warning),
        },
        "items": [item.to_dict() for item in items],
        "raw": raw,
    }


def print_smoke_report(result: Mapping[str, Any], *, stream: Any = None) -> None:
    """Print a compact human-readable smoke-test report."""

    stream = stream or sys.stdout
    summary = _mapping(result.get("summary"))
    print(
        "NECST Operator Console smoke check: "
        f"{summary.get('status')} "
        f"({summary.get('error_count')} error, {summary.get('warning_count')} warning, "
        f"{summary.get('duration_sec')} s)",
        file=stream,
    )
    print(f"URL: {summary.get('url')}", file=stream)
    for item in result.get("items", []):
        if not isinstance(item, Mapping):
            continue
        ok = bool(item.get("ok"))
        severity = str(item.get("severity") or "error")
        marker = "OK" if ok and severity != "warning" else ("WARN" if severity == "warning" else "FAIL")
        print(f"  [{marker}] {item.get('name')}: {item.get('message')}", file=stream)


__all__ = [
    "run_smoke_test",
    "print_smoke_report",
]
